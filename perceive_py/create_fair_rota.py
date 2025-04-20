import datetime
from collections import defaultdict


def fixed_pair_schedule(pairs, start_date, end_date, availability):
    schedule = []
    current_date = start_date
    pair_occurrence = defaultdict(int)  # Tracks the occurrence of pairs per month

    while current_date <= end_date:
        if current_date.weekday() == 0:  # Start on a Monday
            week_schedule = []
            for pair in pairs:
                person1, person2 = pair
                # Check for conflicts and monthly restriction
                pair_month = current_date.strftime(
                    "%Y-%m"
                )  # Get the month (e.g., "2025-03")
                if (
                    availability.get(person1, {}).get(current_date, True)
                    and availability.get(person2, {}).get(current_date, True)
                    and pair_occurrence[(pair, pair_month)] < 1
                ):  # Ensure pair is not repeated in the same month

                    week_schedule.append(pair)
                    # Mark this week as unavailable for both participants
                    availability[person1][current_date] = False
                    availability[person2][current_date] = False
                    # Increment occurrence count for this pair in the current month
                    pair_occurrence[(pair, pair_month)] += 1
                else:
                    print(
                        f"Conflict or monthly limit reached for pair {person1} & {person2} on {current_date}. Skipping..."
                    )
            if week_schedule:
                schedule.append((current_date, week_schedule))
        current_date += datetime.timedelta(days=1)
        # Skip Saturday and Sunday
        while current_date.weekday() > 4:
            current_date += datetime.timedelta(days=1)

    return schedule


def main():
    PAIRS = [
        ("Alpha", "Bravo"),
        ("Charlie", "Delta"),
        ("Echo", "Foxtrot"),
        ("Golf", "Hotel"),
        ("India", "Juliet"),
    ]

    START_DATE = datetime.date(2025, 3, 10)  # Ensure start date is a Monday
    END_DATE = datetime.date(2025, 6, 6)  # Ensure end date is a Friday
    # Main Program
    print("Workweek Scheduling Rota with Monthly Pair Restriction")
    participants = {person for pair in PAIRS for person in pair}
    availability = {
        person: {
            date: True
            for date in (
                START_DATE + datetime.timedelta(days=i)
                for i in range((END_DATE - START_DATE).days + 1)
            )
        }
        for person in participants
    }

    # Generate and display the schedule
    schedule = fixed_pair_schedule(PAIRS, START_DATE, END_DATE, availability)

    print("\nGenerated Schedule:\n")
    for week_date, matches in schedule:
        print(f"Week starting {week_date}:")
        for pair in matches:
            print(f"  {pair[0]} & {pair[1]}")
        print()


if __name__ == "__main__":
    main()
