"""
Tests for Docker configuration
"""
import pytest
import os

class TestDockerConfig:
    """Test Docker setup"""
    
    def test_dockerfile_exists(self):
        """Test that Dockerfile exists"""
        assert os.path.exists("Dockerfile"), "Dockerfile should exist"
    
    def test_docker_compose_exists(self):
        """Test that docker-compose.yml exists"""
        assert os.path.exists("docker-compose.yml"), "docker-compose.yml should exist"
    
    def test_requirements_txt_exists(self):
        """Test that requirements.txt exists"""
        assert os.path.exists("requirements.txt"), "requirements.txt should exist"