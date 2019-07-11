minutes_15 = 15*60

def date_to_string(date,  hours = True):
    """return datetime in string format
        args:
            hours(boolean) : Specifies whether the string should contain the time as well. Default is True"""


    if(hours):
        return f"{date:%Y-%m-%d}T{date:%H:%M:%S}"

    else:
        return f"{date:%Y-%m-%d}"
