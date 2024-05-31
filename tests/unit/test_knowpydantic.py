from perceive_py.knowpydantic import Employee
import pytest


@pytest.fixture
def get_employee():
    return Employee(
        name="John Doe",
        email="jodo@example.com",
        date_of_birth="1998-04-02",
        salary=123_000.00,
        department="IT",
        elected_benefits=True,
    )


def test_employee_creation(get_employee):
    test_result = get_employee.model_dump_json(exclude={"employee_id"})
    expected_result = dict(
        email="jodo@example.com",
        date_of_birth="1998-04-02",
        salary=123_000.00,
        department="IT",
        elected_benefits=True,
    )
    assert test_result == expected_result
