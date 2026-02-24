#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extract file/directory paths from NiFi file-related processors.
Supports: GetFile, PutFile, GetSFTP, PutSFTP, ListSFTP, FetchSFTP, PutHDFS, GetHDFS, FetchHDFS, ListHDFS
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)

# Pattern to find NiFi Expression Language variables: ${varname} or ${varname:function()}
_VAR_PATTERN = re.compile(r'\$\{([^}:]+)(?::[^}]*)?\}')


# Processor type hints for file operations
FILE_READ_HINTS = ("GetFile", "GetSFTP", "FetchSFTP", "ListSFTP", "FetchFile", "ListFile", 
                   "GetHDFS", "FetchHDFS", "ListHDFS")
FILE_WRITE_HINTS = ("PutFile", "PutSFTP", "PutHDFS")

# Property keys for directory/path extraction (order matters - first match wins)
DIRECTORY_KEYS = (
    "Directory",           # PutHDFS, PutFile
    "Input Directory",     # GetFile
    "Remote Path",         # SFTP processors
    "Remote Directory",    # ListSFTP
    "HDFS Directory",      # ListHDFS
)

# Property keys for file filter/pattern extraction
FILE_FILTER_KEYS = (
    "File Filter",         # GetFile, ListFile - regex pattern
    "Filename",            # PutFile, PutHDFS - often uses ${filename}
    "Remote File",         # FetchSFTP
    "Path Filter",         # ListHDFS
)


@dataclass
class FilePathInfo:
    """Information about a file path extracted from a processor."""
    processor_id: str
    processor_name: str
    processor_type: str
    path: str              # Full path (directory + filter if available)
    directory: str         # Just the directory
    file_filter: str | None  # File filter pattern if available
    operation: str         # "read" or "write"


def _is_file_read(ptype: str) -> bool:
    """Check if processor type is a file read operation."""
    return any(h in ptype for h in FILE_READ_HINTS)


def _is_file_write(ptype: str) -> bool:
    """Check if processor type is a file write operation."""
    return any(h in ptype for h in FILE_WRITE_HINTS)


def _get_props(p: dict[str, Any]) -> dict[str, Any]:
    """Extract properties dictionary from a processor."""
    return ((p.get("config") or {}).get("properties") or {}) or (p.get("properties") or {}) or {}


def _as_component(obj: dict[str, Any]) -> dict[str, Any]:
    """Extract the 'component' sub-dict if present."""
    if not isinstance(obj, dict):
        return {}
    return obj.get("component", obj)


def _is_pure_expression(val: str) -> bool:
    """
    Check if a value is purely a NiFi Expression Language variable reference.
    
    Returns True for values like:
    - ${variable}
    - ${variable:function()}
    - ${attribute}
    
    Returns False for values that have static parts like:
    - /path/to/${variable}/file
    - ${variable}/subdir
    """
    if not val:
        return False
    stripped = val.strip()
    # Pure expression: starts with ${ and ends with }
    if stripped.startswith("${") and stripped.endswith("}"):
        # Check if there's no content outside the expression
        # Count balanced braces to handle nested expressions
        depth = 0
        for i, ch in enumerate(stripped):
            if ch == '{' and i > 0 and stripped[i-1] == '$':
                depth += 1
            elif ch == '}':
                depth -= 1
                # If we close the first expression before the end, there's more content
                if depth == 0 and i < len(stripped) - 1:
                    return False
        return depth == 0
    return False


def _resolve_variables(val: str, variables: dict[str, str]) -> str:
    """
    Resolve NiFi Expression Language variables in a string.
    
    Args:
        val: The string potentially containing ${variable} expressions
        variables: A dict mapping variable names to their values
        
    Returns:
        The string with known variables resolved
    """
    if not val or not variables:
        return val
    
    resolved_count = 0
    unresolved: list[str] = []
    
    def replacer(match: re.Match[str]) -> str:
        nonlocal resolved_count, unresolved
        var_name = match.group(1)
        if var_name in variables:
            resolved_count += 1
            return variables[var_name]
        # Keep original expression if variable not found
        unresolved.append(var_name)
        return match.group(0)
    
    result = _VAR_PATTERN.sub(replacer, val)
    if resolved_count > 0:
        log.debug("Resolved %d variable(s) in '%s' -> '%s'", resolved_count, val, result)
    if unresolved:
        log.debug("Unresolved variables: %s", unresolved)
    return result


def _extract_directory(props: dict[str, Any], variables: dict[str, str] | None = None) -> str | None:
    """Extract directory from processor properties. Resolves variables and skips pure expression values."""
    variables = variables or {}
    for key in DIRECTORY_KEYS:
        val = props.get(key)
        if val and isinstance(val, str) and val.strip():
            stripped = val.strip()
            # Try to resolve variables first
            resolved = _resolve_variables(stripped, variables)
            # Skip if it's still purely a variable reference like ${backup_folder}
            if _is_pure_expression(resolved):
                log.debug("Skipping pure expression directory: %s", resolved)
                continue
            return resolved
    return None


def _extract_file_filter(props: dict[str, Any], variables: dict[str, str] | None = None) -> str | None:
    """Extract file filter/pattern from processor properties. Resolves variables."""
    variables = variables or {}
    for key in FILE_FILTER_KEYS:
        val = props.get(key)
        if val and isinstance(val, str) and val.strip():
            stripped = val.strip()
            # Try to resolve variables first
            resolved = _resolve_variables(stripped, variables)
            # Skip NiFi expression language placeholders like ${filename}
            if _is_pure_expression(resolved):
                continue
            return resolved
    return None


def _combine_path(directory: str, file_filter: str | None) -> str:
    """Combine directory and file filter into a full path."""
    if not file_filter:
        return directory
    # Use forward slash for consistency (works for HDFS, SFTP, and Unix paths)
    if directory.endswith("/") or directory.endswith("\\"):
        return f"{directory}{file_filter}"
    return f"{directory}/{file_filter}"


# Keep old function name for backwards compatibility
def _extract_path(props: dict[str, Any], variables: dict[str, str] | None = None) -> str | None:
    """Extract directory/path from processor properties. (Legacy - use _extract_directory)"""
    return _extract_directory(props, variables)


def extract_file_paths_from_processors(
    processors: list[dict[str, Any]],
    variables: dict[str, str] | None = None
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """
    Extract file paths from file-related processors.
    
    Combines directory and file filter into full paths when available.
    Resolves NiFi Expression Language variables using the provided variable registry.
    
    Args:
        processors: List of processor dictionaries
        variables: Optional dict of variable names to their values for resolution
        
    Returns:
        Tuple of (read_paths_by_proc, write_paths_by_proc)
        Each is a dict mapping processor_id -> set of paths
    """
    read_paths: dict[str, set[str]] = {}
    write_paths: dict[str, set[str]] = {}
    variables = variables or {}
    skipped_no_dir = 0
    
    if not processors:
        return read_paths, write_paths
    
    if variables:
        log.debug("Variable registry has %d entries", len(variables))
    
    for raw_p in processors:
        p = _as_component(raw_p)
        ptype = str(p.get("type") or "")
        
        # Skip if not a file processor
        is_read = _is_file_read(ptype)
        is_write = _is_file_write(ptype)
        if not (is_read or is_write):
            continue
        
        pid = p.get("instanceIdentifier") or p.get("id")
        if not pid:
            continue
            
        props = _get_props(p)
        directory = _extract_directory(props, variables)
        
        if not directory:
            skipped_no_dir += 1
            continue
        
        # Get file filter and combine with directory
        file_filter = _extract_file_filter(props, variables)
        full_path = _combine_path(directory, file_filter)
        
        # Add to appropriate dict
        if is_read:
            read_paths.setdefault(pid, set()).add(full_path)
        if is_write:
            write_paths.setdefault(pid, set()).add(full_path)
    
    total_read = sum(len(p) for p in read_paths.values())
    total_write = sum(len(p) for p in write_paths.values())
    if total_read or total_write:
        log.info("Extracted %d read path(s), %d write path(s) from file processors", total_read, total_write)
    if skipped_no_dir:
        log.debug("Skipped %d file processor(s) with unresolved/missing directory", skipped_no_dir)
    
    return read_paths, write_paths


def extract_file_paths_detailed(
    processors: list[dict[str, Any]],
    variables: dict[str, str] | None = None
) -> list[FilePathInfo]:
    """
    Extract detailed file path information from processors.
    
    Args:
        processors: List of processor dictionaries
        variables: Optional dict of variable names to their values for resolution
    
    Returns a list of FilePathInfo objects with full context.
    """
    results: list[FilePathInfo] = []
    variables = variables or {}
    
    if not processors:
        return results
    
    for raw_p in processors:
        p = _as_component(raw_p)
        ptype = str(p.get("type") or "")
        pname = str(p.get("name") or "")
        
        is_read = _is_file_read(ptype)
        is_write = _is_file_write(ptype)
        if not (is_read or is_write):
            continue
        
        pid = p.get("instanceIdentifier") or p.get("id")
        if not pid:
            continue
            
        props = _get_props(p)
        directory = _extract_directory(props, variables)
        
        if not directory:
            continue
        
        file_filter = _extract_file_filter(props, variables)
        full_path = _combine_path(directory, file_filter)
        
        operation = "read" if is_read else "write"
        results.append(FilePathInfo(
            processor_id=pid,
            processor_name=pname,
            processor_type=ptype,
            path=full_path,
            directory=directory,
            file_filter=file_filter,
            operation=operation
        ))
    
    return results


# Legacy function for backward compatibility
def find_sftp_paths(processors: list, variables: dict[str, str] | None = None) -> tuple[list, list]:
    """
    Legacy function - returns lists of (processor_id, path) tuples.
    Deprecated: Use extract_file_paths_from_processors instead.
    """
    read_paths, write_paths = extract_file_paths_from_processors(processors, variables)
    
    sftp_get_paths = [(pid, path) for pid, paths in read_paths.items() for path in paths]
    sftp_put_paths = [(pid, path) for pid, paths in write_paths.items() for path in paths]
    
    return sftp_get_paths, sftp_put_paths