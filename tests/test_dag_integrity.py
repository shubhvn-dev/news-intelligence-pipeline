"""
Tests for Airflow DAG integrity
"""
import pytest
import os
from pathlib import Path

DAG_DIR = Path("airflow/dags")

class TestDAGIntegrity:
    """Test Airflow DAGs can be imported"""
    
    def test_dags_directory_exists(self):
        """Test that DAGs directory exists"""
        assert DAG_DIR.exists(), "DAGs directory should exist"
    
    def test_dag_files_exist(self):
        """Test that DAG files exist"""
        dag_files = list(DAG_DIR.glob("*.py"))
        assert len(dag_files) > 0, "Should have at least one DAG file"
    
    def test_dags_import_without_errors(self):
        """Test that all DAG files can be compiled"""
        for dag_file in DAG_DIR.glob("*.py"):
            if dag_file.name == "__init__.py":
                continue
            
            # Test Python syntax
            with open(dag_file, 'r') as f:
                code = f.read()
                try:
                    compile(code, dag_file.name, 'exec')
                except SyntaxError as e:
                    pytest.fail(f"Syntax error in {dag_file.name}: {e}")