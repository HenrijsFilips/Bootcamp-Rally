# rally data stuff.
# connects to snowflake to manage teams cars races and money.
# split into parts for:
# 1. teams
# 2. cars 
# 3. races
# 4. money

from typing import Any, Dict, List, Optional, Tuple, Union

from snowflake.connector import SnowflakeConnection
from snowflake_db import execute, execute_many, fetch_all, fetch_one_value


# team stuff

def add_team(con: SnowflakeConnection, team_name: str, members: str, budget: float) -> int:
    # add new team.
    # need connection team name members and starting money.
    # returns how many rows changed.
    sql = """
        INSERT INTO BOOTCAMP_RALLY.TEAMS.TEAMS
            (TEAM_NAME, MEMBERS, BUDGET)
        VALUES (%s, %s, %s)
    """
    return execute(con, sql, (team_name, members, budget))


def get_all_teams(con: SnowflakeConnection) -> List[Dict[str, Any]]:
    # get all teams.
    # need a connection.
    # returns list of team data.
    sql = """
        SELECT TEAM_ID, TEAM_NAME, MEMBERS, BUDGET
        FROM BOOTCAMP_RALLY.TEAMS.TEAMS
        ORDER BY TEAM_ID
    """
    return fetch_all(con, sql)


def get_team_by_name(con: SnowflakeConnection, team_name: str) -> Optional[Dict[str, Any]]:
    # find team by name.
    # need connection and team name.
    # returns the team or none if not there.
    sql = """
        SELECT TEAM_ID, TEAM_NAME, MEMBERS, BUDGET
        FROM BOOTCAMP_RALLY.TEAMS.TEAMS
        WHERE TEAM_NAME = %s
    """
    rows = fetch_all(con, sql, (team_name,))
    return rows[0] if rows else None


def update_team_budget_delta(con: SnowflakeConnection, team_id: int, delta: float) -> int:
    # change a team's money.
    # adds the delta to their budget. can be negative.
    # returns number of teams updated.
    sql = """
        UPDATE BOOTCAMP_RALLY.TEAMS.TEAMS
        SET BUDGET = BUDGET + %s
        WHERE TEAM_ID = %s
    """
    return execute(con, sql, (delta, team_id))


def get_team_budget(con: SnowflakeConnection, team_id: int) -> Optional[float]:
    # check how much money a team has.
    # need team id.
    # returns their budget or none if team not found.
    sql = """
        SELECT BUDGET
        FROM BOOTCAMP_RALLY.TEAMS.TEAMS
        WHERE TEAM_ID = %s
    """
    val = fetch_one_value(con, sql, (team_id,))
    return float(val) if val is not None else None


# car stuff

def add_car(
    con: SnowflakeConnection, 
    car_name: str, 
    team_id: int, 
    speed: float, 
    durability: float, 
    acceleration: float
) -> int:
    # add new car.
    # need name team and car stats.
    # returns how many cars added.
    sql = """
        INSERT INTO BOOTCAMP_RALLY.CARS.CARS
            (CAR_NAME, TEAM_ID, SPEED, DURABILITY, ACCELERATION)
        VALUES (%s, %s, %s, %s, %s)
    """
    return execute(con, sql, (car_name, team_id, speed, durability, acceleration))


def get_all_cars(con: SnowflakeConnection) -> List[Dict[str, Any]]:
    # get all cars and their team info.
    # need connection.
    # returns list of car data.
    sql = """
        SELECT
            c.CAR_ID,
            c.CAR_NAME,
            c.TEAM_ID,
            t.TEAM_NAME,
            c.SPEED,
            c.DURABILITY,
            c.ACCELERATION
        FROM BOOTCAMP_RALLY.CARS.CARS c
        LEFT JOIN BOOTCAMP_RALLY.TEAMS.TEAMS t
            ON c.TEAM_ID = t.TEAM_ID
        ORDER BY c.CAR_ID
    """
    return fetch_all(con, sql)


# race stuff

def create_race(con: SnowflakeConnection, track_name: str) -> Optional[int]:
    # make a new race.
    # need track name.
    # returns race id or none if failed.
    sql = """
        INSERT INTO BOOTCAMP_RALLY.RACES.RACES
            (TRACK_NAME)
        VALUES (%s)
    """
    execute(con, sql, (track_name,))
    # get race id i made
    race_id = fetch_one_value(
        con,
        "SELECT MAX(RACE_ID) FROM BOOTCAMP_RALLY.RACES.RACES"
    )
    return int(race_id) if race_id is not None else None


def set_race_winner(con: SnowflakeConnection, race_id: int, winner_team_id: int) -> int:
    # mark which team won a race.
    # need race id and winner team id.
    # returns number of races updated.
    sql = """
        UPDATE BOOTCAMP_RALLY.RACES.RACES
        SET WINNER_TEAM_ID = %s
        WHERE RACE_ID = %s
    """
    return execute(con, sql, (winner_team_id, race_id))


def insert_race_results(
    con: SnowflakeConnection, 
    race_id: int, 
    results: List[Tuple[int, float, int]]
) -> int:
    # save race results for all cars.
    # need race id and list of car results.
    # returns how many result rows added.
    sql = """
        INSERT INTO BOOTCAMP_RALLY.RACES.RACE_RESULTS
            (RACE_ID, CAR_ID, TIME_TAKEN, POSITION)
        VALUES (%s, %s, %s, %s)
    """
    # make list
    params = []
    for (car_id, time_taken, position) in results:
        params.append((race_id, car_id, time_taken, position))
    
    return execute_many(con, sql, params)


def get_race_results(con: SnowflakeConnection, race_id: int) -> List[Dict[str, Any]]:
    # get results for a specific race.
    # includes car and team details.
    # returns results sorted by position.
    sql = """
        SELECT rr.RESULT_ID,
               rr.RACE_ID,
               rr.CAR_ID,
               c.CAR_NAME,
               c.TEAM_ID,
               t.TEAM_NAME,
               rr.TIME_TAKEN,
               rr.POSITION
        FROM BOOTCAMP_RALLY.RACES.RACE_RESULTS rr
        LEFT JOIN BOOTCAMP_RALLY.CARS.CARS c
            ON rr.CAR_ID = c.CAR_ID
        LEFT JOIN BOOTCAMP_RALLY.TEAMS.TEAMS t
            ON c.TEAM_ID = t.TEAM_ID
        WHERE rr.RACE_ID = %s
        ORDER BY rr.POSITION ASC
    """
    return fetch_all(con, sql, (race_id,))


# money stuff

def pay_participation_fee_for_all_teams_with_cars(con: SnowflakeConnection, fee: float) -> int:
    # make teams pay for joining race.
    # takes money from all teams with cars.
    # returns how many teams paid.
    sql = """
        UPDATE BOOTCAMP_RALLY.TEAMS.TEAMS t
        SET t.BUDGET = t.BUDGET - %s
        WHERE t.TEAM_ID IN (
            SELECT DISTINCT TEAM_ID
            FROM BOOTCAMP_RALLY.CARS.CARS
            WHERE TEAM_ID IS NOT NULL
        )
    """
    return execute(con, sql, (fee,))


def credit_winner_prize(con: SnowflakeConnection, winner_team_id: int, prize: float) -> int:
    # give prize money to winner.
    # adds cash to winning team budget.
    # returns how many teams got paid.
    sql = """
        UPDATE BOOTCAMP_RALLY.TEAMS.TEAMS
        SET BUDGET = BUDGET + %s
        WHERE TEAM_ID = %s
    """
    return execute(con, sql, (prize, winner_team_id))