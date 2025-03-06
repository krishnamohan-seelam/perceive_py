import random
from datetime import datetime, timedelta


def create_fixed_rota_with_dates(pairs, start_date, weeks):
    """ """
    rota = []
    if len(pairs) < 1:
        return None
    # if weeks > len(pairs):
    #     # Check  if is it possible to have non-repeating rota for given weeks
    #     print("not possible to have  non-repeating rota for given weeks")
    #     return None
    for week in range(weeks):
        rota_start = start_date + timedelta(weeks=week)
        rota_end = rota_start + timedelta(days=4)
        rota.append(
            (
                rota_start.date().isoformat(),
                rota_end.date().isoformat(),
                pairs[week % len(pairs)],
            )
        )
    return rota


def main():
    PAIRS = [
        ("Alpha", "Bravo"),
        ("Charlie", "Delta"),
        ("Echo", "Foxtrot"),
        ("Golf", "Hotel"),
        ("India", "Juliet"),
    ]
    WEEKS = 10
    START_DATE = datetime(2025, 3, 31)
    rota_with_dates = create_fixed_rota_with_dates(PAIRS, START_DATE, WEEKS)

    if isinstance(rota_with_dates, list):
        for week, (start_date, end_date, pair) in enumerate(rota_with_dates):
            print(f"Week {week} - {start_date} to {end_date}: {pair[0]} and {pair[1] }")


if __name__ == "__main__":
    main()
