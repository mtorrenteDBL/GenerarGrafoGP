"""
Extract and format Neo4j database schema for LLM consumption.

This script queries the Neo4j database to extract:
- Node labels and their properties
- Relationship types and their properties
- Cardinality statistics
- Connection patterns

Output is formatted as human-readable text suitable for passing to LLMs.
"""

import json
import sys
from typing import Dict, List, Set, Tuple, Any
from neo4j import GraphDatabase
from os import getenv
from dotenv import load_dotenv, find_dotenv
import argparse

# Load environment variables
load_dotenv(find_dotenv(), override=True)


class Neo4jSchemaExtractor:
    """Extracts and formats Neo4j database schema."""

    def __init__(self, uri: str, user: str, password: str):
        """Initialize Neo4j connection."""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """Close the driver connection."""
        self.driver.close()

    def get_node_labels(self) -> Dict[str, Dict[str, Any]]:
        """
        Extract all node labels and their properties.
        
        Returns:
            Dict mapping label names to property information
        """
        # First, get all labels
        labels_query = "MATCH (n) RETURN DISTINCT labels(n) as labels"
        
        result = {}
        all_labels = set()
        
        with self.driver.session() as session:
            records = session.run(labels_query)
            for record in records:
                label_list = record['labels']
                if label_list:
                    all_labels.add(label_list[0])
        
        # For each label, get a sample node and extract its properties
        for label in all_labels:
            result[label] = {}
            # Escape label names with backticks in case they contain spaces
            escaped_label = f"`{label}`" if " " in label else label
            sample_query = f"MATCH (n:{escaped_label}) RETURN keys(n) as keys, n LIMIT 1"
            
            with self.driver.session() as session:
                record = session.run(sample_query).single()
                if record:
                    keys = record['keys'] or []
                    node = record['n']
                    
                    for key in keys:
                        value = node.get(key)
                        # Determine type from the actual value
                        if value is None:
                            prop_type = 'null'
                        elif isinstance(value, bool):
                            prop_type = 'boolean'
                        elif isinstance(value, int):
                            prop_type = 'integer'
                        elif isinstance(value, float):
                            prop_type = 'float'
                        elif isinstance(value, list):
                            prop_type = 'list'
                        elif isinstance(value, str):
                            prop_type = 'string'
                        else:
                            prop_type = 'unknown'
                        
                        result[label][key] = prop_type
        
        # Get cardinality stats
        for label in result.keys():
            # Escape label names with backticks in case they contain spaces
            escaped_label = f"`{label}`" if " " in label else label
            count_query = f"MATCH (n:{escaped_label}) RETURN count(n) as count"
            with self.driver.session() as session:
                record = session.run(count_query).single()
                if record:
                    # Store in the label dict for reference
                    result[label]['_node_count'] = record['count']
        
        return result

    def get_relationship_types(self) -> Dict[str, Dict[str, Any]]:
        """
        Extract all relationship types and their properties.
        
        Returns:
            Dict mapping relationship types to property and cardinality info
        """
        # Get all relationship types
        rel_types_query = "MATCH ()-[r]->() RETURN DISTINCT type(r) as rel_type"
        
        result = {}
        rel_types = set()
        
        with self.driver.session() as session:
            records = session.run(rel_types_query)
            for record in records:
                rel_type = record['rel_type']
                if rel_type:
                    rel_types.add(rel_type)
        
        # For each relationship type, get properties and patterns
        for rel_type in rel_types:
            result[rel_type] = {
                'properties': {},
                'patterns': []
            }
            
            # Get a sample relationship to extract properties
            sample_query = f"MATCH ()-[r:{rel_type}]->() RETURN keys(r) as keys, r LIMIT 1"
            
            with self.driver.session() as session:
                record = session.run(sample_query).single()
                if record:
                    keys = record['keys'] or []
                    rel = record['r']
                    
                    for key in keys:
                        value = rel.get(key)
                        # Determine type from the actual value
                        if value is None:
                            prop_type = 'null'
                        elif isinstance(value, bool):
                            prop_type = 'boolean'
                        elif isinstance(value, int):
                            prop_type = 'integer'
                        elif isinstance(value, float):
                            prop_type = 'float'
                        elif isinstance(value, list):
                            prop_type = 'list'
                        elif isinstance(value, str):
                            prop_type = 'string'
                        else:
                            prop_type = 'unknown'
                        
                        result[rel_type]['properties'][key] = prop_type
            
            # Get relationship patterns and counts
            pattern_query = f"""
            MATCH (a)-[r:{rel_type}]->(b)
            RETURN
                labels(a)[0] as src_label,
                labels(b)[0] as dst_label,
                count(r) as count
            ORDER BY count DESC
            LIMIT 10
            """
            
            with self.driver.session() as session:
                records = session.run(pattern_query)
                patterns = []
                
                for record in records:
                    src_label = record['src_label'] or "Unknown"
                    dst_label = record['dst_label'] or "Unknown"
                    count = record['count']
                    
                    patterns.append({
                        'source': src_label,
                        'target': dst_label,
                        'count': count
                    })
                
                result[rel_type]['patterns'] = patterns
            
            # Get total count for this relationship type
            total_count_query = f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count"
            with self.driver.session() as session:
                record = session.run(total_count_query).single()
                if record:
                    result[rel_type]['total_count'] = record['count']
        
        return result

    def get_node_count(self) -> int:
        """Get total number of nodes in the database."""
        query = "MATCH (n) RETURN count(n) as count"
        with self.driver.session() as session:
            record = session.run(query).single()
            return record['count'] if record else 0

    def get_relationship_count(self) -> int:
        """Get total number of relationships in the database."""
        query = "MATCH ()-[r]->() RETURN count(r) as count"
        with self.driver.session() as session:
            record = session.run(query).single()
            return record['count'] if record else 0


def format_schema_for_llm(
    node_labels: Dict[str, Dict[str, Any]],
    relationships: Dict[str, Dict[str, Any]],
    node_count: int,
    rel_count: int,
    output_format: str = "text"
) -> str:
    """
    Format extracted schema in a human-readable way for LLM consumption.
    
    Args:
        node_labels: Node labels and properties
        relationships: Relationship types and patterns
        node_count: Total count of nodes
        rel_count: Total count of relationships
        output_format: 'text' for markdown-like, 'json' for JSON
        
    Returns:
        Formatted schema string
    """
    
    if output_format == "json":
        return json.dumps({
            'metadata': {
                'total_nodes': node_count,
                'total_relationships': rel_count
            },
            'node_labels': node_labels,
            'relationship_types': relationships
        }, indent=2, default=str)
    
    # Text format
    lines = []
    lines.append("=" * 80)
    lines.append("NEO4J DATABASE SCHEMA")
    lines.append("=" * 80)
    lines.append("")
    
    # Node Labels Section
    lines.append("NODE LABELS")
    lines.append("-" * 80)
    
    for label in sorted(node_labels.keys()):
        props = node_labels[label]
        node_count_label = props.pop('_node_count', 0)
        
        lines.append(f"\n[{label}]")
        lines.append(f"  Properties:")
        
        for prop_name in sorted(props.keys()):
            prop_info = props[prop_name]
            if isinstance(prop_info, dict):
                prop_type = prop_info.get('type', 'unknown')
            else:
                prop_type = prop_info
            
            lines.append(f"    • {prop_name} ({prop_type})")
    
    lines.append("\n")
    
    # Relationship Types Section
    lines.append("RELATIONSHIP TYPES")
    lines.append("-" * 80)
    
    for rel_type in sorted(relationships.keys()):
        rel_info = relationships[rel_type]
        patterns = rel_info.get('patterns', [])
        properties = rel_info.get('properties', {})
        
        lines.append(f"\n[{rel_type}]")
        
        if properties:
            lines.append(f"  Properties:")
            for prop_name in sorted(properties.keys()):
                prop_type = properties[prop_name]
                lines.append(f"    • {prop_name} ({prop_type})")
        else:
            lines.append(f"  Properties: None")
        
        lines.append(f"  Connection Patterns:")
        for pattern in patterns:
            src = pattern['source']
            dst = pattern['target']
            lines.append(f"    • ({src}) -[{rel_type}]-> ({dst})")
    
    
    
    return "\n".join(lines)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Extract and display Neo4j database schema"
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output file path (if not specified, prints to stdout)"
    )
    parser.add_argument(
        "--format",
        "-f",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)"
    )
    parser.add_argument(
        "--uri",
        default=getenv('NEO4J_HOST'),
        help="Neo4j URI (default: NEO4J_HOST env var)"
    )
    parser.add_argument(
        "--user",
        default=getenv('NEO4J_USER'),
        help="Neo4j username (default: NEO4J_USER env var)"
    )
    parser.add_argument(
        "--password",
        default=getenv('NEO4J_PASS'),
        help="Neo4j password (default: NEO4J_PASS env var)"
    )
    
    args = parser.parse_args()
    
    # Validate required parameters
    if not all([args.uri, args.user, args.password]):
        print("Error: Missing Neo4j connection parameters.", file=sys.stderr)
        print("Provide via --uri, --user, --password or NEO4J_HOST, NEO4J_USER, NEO4J_PASS", 
              file=sys.stderr)
        sys.exit(1)
    
    print("Connecting to Neo4j...", file=sys.stderr)
    
    try:
        extractor = Neo4jSchemaExtractor(args.uri, args.user, args.password)
        
        print("Extracting schema...", file=sys.stderr)
        node_labels = extractor.get_node_labels()
        relationships = extractor.get_relationship_types()
        node_count = extractor.get_node_count()
        rel_count = extractor.get_relationship_count()
        
        extractor.close()
        
        print("Formatting schema...", file=sys.stderr)
        output = format_schema_for_llm(node_labels, relationships, node_count, rel_count, args.format)
        
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(output)
            print(f"Schema saved to: {args.output}", file=sys.stderr)
        else:
            print(output)
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
