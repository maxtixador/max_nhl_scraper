from datetime import datetime


def is_valid_date(date_str : str):
    """
    Checks if a given date is valid.
    """

    if date_str == "now":
        date_str = datetime.now().strftime("%Y-%m-%d")
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False 
    

def find_key_by_value_in_list(dictionary, value):
    """
    Finds the key of a given value in a dictionary of lists.
    """
    for k, v in dictionary.items():
        if value in v:
            return k
    return None

def adjust_x_coord(row):
    if row['homeTeamDefendingSide'] == 'left':
        return row['details.xCoord']
    else:
        # Flip the x-coordinate within the range -100 to 100
        return 100 - row['details.xCoord']
