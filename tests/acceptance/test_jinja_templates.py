"""
Test suite for validating Jinja2 templates in Airflow DAG files.

This test module ensures that all Jinja2 templates used in DAG files are syntactically
correct and free from common typos. It helps catch errors like:
- Spaces in variable names (e.g., 'task_ instance' instead of 'task_instance')
- Invalid Jinja2 syntax
- Common typos in Airflow template variables

These tests run during the acceptance test phase and will fail the build if any
template errors are detected, preventing deployment of broken DAGs.
"""
import pytest
import re
import os
from pathlib import Path
from jinja2 import Environment, TemplateSyntaxError

DAG_FOLDER = 'dags'


def get_all_dag_files():
    """Get all Python files in the DAG folder."""
    dag_path = Path(DAG_FOLDER)
    return list(dag_path.glob('*.py'))


def extract_jinja_templates(file_content):
    """Extract all Jinja2 templates from Python file content."""
    # Pattern to match Jinja2 templates in strings
    # Matches {{ ... }} and {% ... %} patterns
    jinja_pattern = re.compile(r'["\']([^"\']*\{\{[^}]*\}\}[^"\']*)["\']|["\']([^"\']*\{%[^%]*%\}[^"\']*)["\']')

    templates = []
    for match in jinja_pattern.finditer(file_content):
        template = match.group(1) or match.group(2)
        if template:
            # Unescape the quotes that are escaped in Python strings
            template = template.replace(r'\"', '"').replace(r"\'", "'")
            templates.append(template)

    return templates


def test_jinja_template_syntax():
    """Test that all Jinja2 templates in DAG files have valid syntax."""
    dag_files = get_all_dag_files()
    assert len(dag_files) > 0, "No DAG files found!"

    env = Environment()
    failed_templates = []

    for dag_file in dag_files:
        file_content = dag_file.read_text()
        templates = extract_jinja_templates(file_content)

        for template in templates:
            try:
                # Try to parse the template
                env.parse(template)
            except TemplateSyntaxError as e:
                failed_templates.append(
                    f"\nFile: {dag_file.name}\n"
                    f"Template: {template}\n"
                    f"Error: {str(e)}"
                )

    if failed_templates:
        error_msg = "\n".join(failed_templates)
        pytest.fail(f"Found Jinja2 template syntax errors:\n{error_msg}")


def test_no_spaces_in_jinja_variables():
    """Test that Jinja2 templates don't have spaces in variable names like 'task_ instance'."""
    dag_files = get_all_dag_files()
    assert len(dag_files) > 0, "No DAG files found!"

    # Pattern to detect spaces within Jinja2 expressions
    # This catches cases like {{ task_ instance.xcom_pull }}
    space_in_variable_pattern = re.compile(r'\{\{[^}]*\w+\s+\w+[^}]*\}\}')

    failed_checks = []

    for dag_file in dag_files:
        file_content = dag_file.read_text()

        # Find all matches
        matches = space_in_variable_pattern.findall(file_content)

        for match in matches:
            # Filter out intentional spaces (like in filters or after commas)
            # But catch unintentional spaces in variable names
            if re.search(r'\w+_\s+\w+', match):  # Detects cases like 'task_ instance'
                # Get line number for better error reporting
                line_num = file_content[:file_content.find(match)].count('\n') + 1
                failed_checks.append(
                    f"\nFile: {dag_file.name} (line ~{line_num})\n"
                    f"Found space in variable name: {match}\n"
                )

    if failed_checks:
        error_msg = "\n".join(failed_checks)
        pytest.fail(f"Found Jinja2 templates with spaces in variable names:\n{error_msg}")


def test_common_jinja_typos():
    """Test for common Jinja2 typos and mistakes."""
    dag_files = get_all_dag_files()
    assert len(dag_files) > 0, "No DAG files found!"

    common_typos = [
        (r'task_\s+instance', 'task_ instance (should be task_instance)'),
        (r'dag_\s+run', 'dag_ run (should be dag_run)'),
        (r'xcom_\s+pull', 'xcom_ pull (should be xcom_pull)'),
        (r'xcom_\s+push', 'xcom_ push (should be xcom_push)'),
    ]

    failed_checks = []

    for dag_file in dag_files:
        file_content = dag_file.read_text()

        for pattern, description in common_typos:
            matches = re.finditer(pattern, file_content)
            for match in matches:
                # Get line number
                line_num = file_content[:match.start()].count('\n') + 1
                failed_checks.append(
                    f"\nFile: {dag_file.name} (line {line_num})\n"
                    f"Found typo: {description}\n"
                    f"Context: {match.group()}\n"
                )

    if failed_checks:
        error_msg = "\n".join(failed_checks)
        pytest.fail(f"Found common typos in DAG files:\n{error_msg}")
