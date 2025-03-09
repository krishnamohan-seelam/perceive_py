"""
The Python script generates a fixed rota schedule with pairs of values for each week based on user
input for start date and number of rota days.
"""

from datetime import date, timedelta
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter(
    "{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# isoweekday(Monday =1 Sunday=6)

DAYS_IN_WEEK = 7
WORKDAYS_IN_WEEK = 5
ISO_WEEKDAY_OFFSET = 1


def str_to_date(datestr: str):
    """
    The function str_to_date converts an ISO formatted date string to a date object, using the
    current date if the input is invalid.

    :param datestr: date string in iso format
    :return: date
    """
    try:
        iso_date = date.fromisoformat(datestr)
    except ValueError as ve:
        logger.info(f"{ve} using current date")
        iso_date = date.today()

    finally:
        return iso_date


def get_next_monday(current_date: date):
    """
    The function `get_next_monday` returns the date of the next Monday after the given current date.
    If the current date is already a Monday,it will return the same current date.

    It uses isoweekday(Monday =1 Sunday=6) for calculation.

    :param current_date: The `current_date` parameter for which you want to find the next Monday
    :return: the next Monday date after the input current date.
    """
    weekday = current_date.isoweekday()
    days_offset = (DAYS_IN_WEEK - weekday) + ISO_WEEKDAY_OFFSET
    if days_offset < DAYS_IN_WEEK:
        return current_date + timedelta(days_offset)
    return current_date


def round_rota_days(rota_days: int, workdays: int = WORKDAYS_IN_WEEK):
    """
    This function calculates the rounded number of workdays in a given number of rota days.

    :param rota_days: The `rota_days` parameter represents the total number of days in rota.
    :param workdays: The `workdays` parameter represents the number of workdays in a week.
    :return: the rounded number of rota days based on the number of workdays in a week.
    """
    return workdays * round(rota_days / workdays)


def create_fixed_rota_with_dates(pairs, start_date, end_date):
    """
    The function creates a fixed rota schedule with dates for pairs of items within a specified start
    and end date range.

    :param pairs: The `pairs` parameter represents a list of pairs that will be assigned to each week in the rota.
    Each pair can be a tuple, list, or any other data structure that contains the information you want to assign to each week.
    :param start_date: The `start_date` parameter starting date for the rota schedule.
    :param end_date: The `end_date` parameter represents the end date for the fixed rota schedule.
    :return: list of tuples representing the fixed rota schedule with start date, end date, and the pair assigned for that week.
    """
    rota = []
    if len(pairs) < 1:
        return None
    weeks = 0
    rota_start = start_date
    while rota_start <= end_date:
        rota_end = rota_start + timedelta(days=4)
        rota.append(
            (
                rota_start.isoformat(),
                rota_end.isoformat(),
                pairs[weeks % len(pairs)],
            )
        )
        weeks = weeks + 1
        rota_start = start_date + timedelta(weeks=weeks)

    return rota


def main():
    inp_date = input("Enter rota start date(YYYY-MM-DD): ")
    inp_rota_days = int(input("Enter rota days(will be rounded to nearest workdays): "))
    iso_date = str_to_date(inp_date)
    start_date = get_next_monday(iso_date)
    rota_days = round_rota_days(inp_rota_days)
    end_date = start_date + timedelta(rota_days)
    print(f"Start Date:{start_date}")
    print(f"End Date:{end_date}")
    PAIRS = [
        ("Alpha", "Bravo"),
        ("Charlie", "Delta"),
        ("Echo", "Foxtrot"),
        ("Golf", "Hotel"),
        ("India", "Juliet"),
    ]
    rota_with_dates = create_fixed_rota_with_dates(PAIRS, start_date, end_date)
    if isinstance(rota_with_dates, list):
        for week, (rota_start_date, rota_end_date, pair) in enumerate(rota_with_dates):
            print(
                f"Week {week} - {rota_start_date} to {rota_end_date}: {pair[0]} and {pair[1]}"
            )


if __name__ == "__main__":
    main()
