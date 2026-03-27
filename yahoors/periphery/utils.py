import datetime as dt


def clean_tickers(tickers: list[str]) -> list[str]: 
    mapping = {"/": "-", "\\": "-", "@": "-", ".": "-"}
    return [ticker.translate(str.maketrans(mapping)) for ticker in tickers]

def is_stale(data: dict[str, list[str]], stale_threshold: dt.timedelta, date_format: str = "%Y-%m-%d %H:%M:%S") -> dict[str, bool]:
    return {date: (dt.datetime.now() - dt.datetime.strptime(date, date_format)) > stale_threshold for date in data}

# def is_stale(data: dict[str, str], stale_threshold: dt.timedelta, date_format: str = "%Y-%m-%d %H:%M:%S") -> dict[str, bool]: 
#     return {ticker: (dt.datetime.now() - dt.datetime.strptime(date, date_format)) > stale_threshold for ticker, date in data.items()}


def list_difference(list1: list, list2: list) -> list:
    """
    Return a new list containing every element that is in *list2* but not in *list1*.
    Order from *list2* is preserved.

    Parameters
    ----------
    list1 : list
        Reference list – items that should be excluded.
    list2 : list
        List from which to pick items that are not in *list1*.

    Returns
    -------
    list
        The difference, in the original order from *list2*.
    """
    # Build a set for fast membership checks (O(1) per look‑up)
    seen_in_first = set(list1)

    # Preserve order: iterate over list2 and keep only the missing ones
    return [item for item in list2 if item not in seen_in_first]


def list_similarity(list1: list, list2: list):
    """
    Return a new list containing every element that is in both *list2* and *list1*.
    Order from *list2* is preserved.

    Parameters
    ----------
    list1 : list
        Reference list – items that should be excluded.
    list2 : list
        List from which to pick items that are not in *list1*.

    Returns
    -------
    list
        The difference, in the original order from *list2*.
    """
    # Build a set for fast membership checks (O(1) per look‑up)
    seen_in_first = set(list1)

    # Preserve order: iterate over list2 and keep only the missing ones
    return [item for item in list2 if item in seen_in_first]