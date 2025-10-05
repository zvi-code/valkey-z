# src/workload/registry.py
"""Workload registration and discovery."""

from __future__ import annotations

from typing import Dict, Type, Callable, List, Optional
import logging
import importlib
import inspect

from .base import BaseWorkload

logger = logging.getLogger(__name__)


class WorkloadRegistry:
    """Registry for available workloads."""
    
    _workloads: Dict[str, Type[BaseWorkload]] = {}
    _descriptions: Dict[str, str] = {}
    
    @classmethod
    def register(cls, 
                name: str, 
                workload_class: Type[BaseWorkload],
                description: Optional[str] = None) -> None:
        """
        Register a workload class.
        
        Args:
            name: Unique name for the workload
            workload_class: Workload class (must inherit from BaseWorkload)
            description: Optional description of the workload
        """
        # Validate workload class
        if not inspect.isclass(workload_class):
            raise TypeError(f"Expected class, got {type(workload_class)}")
        
        if not issubclass(workload_class, BaseWorkload):
            raise TypeError(f"{workload_class.__name__} must inherit from BaseWorkload")
        
        # Check for duplicate registration
        if name in cls._workloads:
            logger.warning(f"Overwriting existing workload registration: {name}")
        
        # Register the workload
        cls._workloads[name] = workload_class
        cls._descriptions[name] = description or workload_class.__doc__ or "No description available"
        
        logger.info(f"Registered workload: {name} -> {workload_class.__name__}")
        
    @classmethod
    def get(cls, name: str) -> Type[BaseWorkload]:
        """
        Get a workload class by name.
        
        Args:
            name: Workload name
            
        Returns:
            Workload class
            
        Raises:
            KeyError: If workload not found
        """
        if name not in cls._workloads:
            available = ", ".join(cls._workloads.keys())
            raise KeyError(f"Workload '{name}' not found. Available workloads: {available}")
        
        return cls._workloads[name]
    
    @classmethod
    def create_instance(cls, name: str, **kwargs) -> BaseWorkload:
        """
        Create a workload instance by name.
        
        Args:
            name: Workload name
            **kwargs: Arguments to pass to workload constructor
            
        Returns:
            Workload instance
        """
        workload_class = cls.get(name)
        return workload_class(**kwargs)
        
    @classmethod
    def list_workloads(cls) -> List[str]:
        """
        List all registered workload names.
        
        Returns:
            Sorted list of workload names
        """
        return sorted(cls._workloads.keys())
    
    @classmethod
    def get_workload_info(cls) -> List[Dict[str, str]]:
        """
        Get information about all registered workloads.
        
        Returns:
            List of dicts with workload information
        """
        info = []
        for name in cls.list_workloads():
            info.append({
                "name": name,
                "class": cls._workloads[name].__name__,
                "module": cls._workloads[name].__module__,
                "description": cls._descriptions[name]
            })
        return info
    
    @classmethod
    def clear(cls) -> None:
        """Clear all registered workloads (mainly for testing)."""
        cls._workloads.clear()
        cls._descriptions.clear()
    
    @classmethod
    def auto_discover(cls, package: str = "search_delete_test.workload") -> None:
        """
        Auto-discover and register workloads from a package.
        
        Args:
            package: Package name to search for workloads
        """
        # Import the package
        try:
            pkg = importlib.import_module(package)
        except ImportError as e:
            logger.error(f"Failed to import package {package}: {e}")
            return
        
        # Get all modules in the package
        import pkgutil
        
        for importer, modname, ispkg in pkgutil.iter_modules(pkg.__path__, prefix=f"{package}."):
            if ispkg:
                continue
                
            try:
                module = importlib.import_module(modname)
                
                # Look for BaseWorkload subclasses
                for name, obj in inspect.getmembers(module, inspect.isclass):
                    if (issubclass(obj, BaseWorkload) and 
                        obj is not BaseWorkload and
                        obj.__module__ == modname):
                        
                        # Use class name as workload name (lowercase)
                        workload_name = name.lower().replace("workload", "")
                        if not workload_name:
                            workload_name = name.lower()
                        
                        # Skip if already registered
                        if workload_name not in cls._workloads:
                            cls.register(workload_name, obj)
                            
            except Exception as e:
                logger.error(f"Failed to import module {modname}: {e}")


def register_workload(name: str, description: Optional[str] = None) -> Callable:
    """
    Decorator to register a workload class.
    
    Args:
        name: Unique name for the workload
        description: Optional description
        
    Returns:
        Decorator function
    """
    def decorator(cls: Type[BaseWorkload]) -> Type[BaseWorkload]:
        WorkloadRegistry.register(name, cls, description)
        return cls
    
    return decorator


# Register built-in workloads
def register_builtin_workloads():
    """Register all built-in workloads."""
    from .ingest import IngestWorkload
    from .query import QueryWorkload
    from .shrink import ShrinkWorkload
    
    WorkloadRegistry.register(
        "ingest", 
        IngestWorkload,
        "Parallel vector insertion into Valkey"
    )
    
    WorkloadRegistry.register(
        "query",
        QueryWorkload,
        "KNN query execution with recall measurement"
    )
    
    WorkloadRegistry.register(
        "shrink",
        ShrinkWorkload,
        "Random deletion of vectors to shrink the index"
    )
    
    logger.info(f"Registered {len(WorkloadRegistry._workloads)} built-in workloads")


# Auto-register on import
register_builtin_workloads()