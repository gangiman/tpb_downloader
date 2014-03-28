from tpb import TPB
import json
from datetime import date as _date
from datetime import datetime as _datetime
from datetime import timedelta
import logging
from time import sleep
from itertools import chain
from itertools import takewhile
from operator import attrgetter

time_shift = 0


class datetime(_datetime):

    @classmethod
    def now(cls, tz=None):
        return super(datetime, cls).now(tz=tz) - timedelta(days=time_shift)


class date(_date):

    @classmethod
    def today(cls):
        return super(date, cls).today() - timedelta(days=time_shift)



logger = logging.getLogger('tpb_downloader')
handler = logging.FileHandler('downloader.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

days = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")

days_map = dict(zip(days, range(7)))

site_handle = TPB("http://thepiratebay.org/")


def str_from_ts(ts):
    return datetime.fromtimestamp(ts).strftime("%c")


def read_config():
    with open("config.json", "r") as fh:
        config = json.loads(fh.read())
    return config


def check_for_torrent(query, config):
    list_of_torrents = list(site_handle.search(query))
    if list_of_torrents:
        print("Found {} torrents for request '{}'".format(
            len(list_of_torrents),
            query
        ))
        print("\n".join(map(str, list_of_torrents)))
    else:
        print("No torrents found for '{}'".format(query))
    list_from_releasers = [t for t in list_of_torrents
                           if t.user in config["trusted_release_groups"]]
    if list_from_releasers:
        return sorted(list_from_releasers, key=attrgetter("seeders", "leechers"))[-1]


def linspace(start, stop, n):
    if n == 1:
        yield stop
        return
    h = float(stop - start) / float(n - 1)
    for i in range(n):
        yield start + h * i


def get_iterator(start, end, steps, func, *args):
    ls = linspace(start, end, steps)
    now = datetime.now().timestamp()
    fls = list(takewhile(lambda m: m < now, ls))
    if fls:
        next_moment = fls[-1]
    else:
        next_moment = next(ls)
    while True:
        if now > next_moment:
            next_moment = next(ls)
            yield next_moment, func(*args)
        else:
            yield next_moment, None
        now = datetime.now().timestamp()


def iterator_constructor(show, config, yesterday, day_before_template):
    steps_in_variance = config["steps_in_variance"]
    steps_outside_variance = config["steps_outside_variance"]
    outside_variance_period = config["outside_variance_period"]
    episode_query = day_before_template.format(show)
    yts = yesterday.timestamp()
    #TODO: updatable mean and std ( http://math.stackexchange.com/a/297148 )
    mean = 36000
    variance = 7200
    in_variance_iterator = get_iterator(
        yts + mean - variance,
        yts + mean + variance,
        steps_in_variance,
        check_for_torrent,
        episode_query,
        config
    )
    outside_variance_iterator = get_iterator(
        yts + mean + variance,
        yts + mean + variance + outside_variance_period,
        steps_outside_variance,
        check_for_torrent,
        episode_query,
        config
    )
    for next_moment, result in chain(in_variance_iterator, outside_variance_iterator):
        yield next_moment, result
    yield None, None


def parse_airs(airs):
    if "," in airs:
        results = (d if isinstance(d, set) else {d}
                   for d in map(parse_airs, airs.split(",")))
        return set.union(*tuple(results))
    elif "-" in airs:
        start_end = airs.split("-")
        start_end_int = sorted(map(lambda x: days_map.get(x.lower()), start_end))
        return set(range(start_end_int[0], start_end_int[1] + 1))
    else:
        return {days_map.get(airs.lower(), -1)}


def do_stuff(config, checkers, new_day=False):
    if new_day:
        today_date = date.today()
        print(today_date.strftime("It's a new day (%b %d), creating new checks."))
        yesterday = datetime.combine(today_date, datetime.min.time()) - timedelta(seconds=1)
        day_before_template = yesterday.strftime("{} %Y %m %d")
        weekday = yesterday.weekday()
        for show, params in config["shows"].items():
            print("Creating checker for '{}'".format(show))
            if weekday in parse_airs(params["airs"]):
                print("Show airs during {} and today is {}".format(params["airs"], days[weekday]))
                if params["chosen_release"] != "480p":
                    dbt_with_quality = day_before_template + " " + params["chosen_release"]
                else:
                    dbt_with_quality = day_before_template
                print("Final query will be '{}'".format(dbt_with_quality.format(show)))
                checkers.append(iterator_constructor(show, config, yesterday, dbt_with_quality))
    results = []
    next_check = []
    for iterator in checkers[:]:
        next_moment, result = next(iterator)
        if result is not None:
            results.append(result)
            checkers.remove(iterator)
        else:
            if next_moment is not None:
                next_check.append(next_moment)
            else:
                checkers.remove(iterator)
    return min(next_check), results


def main(config, new_day=False):
    base_today = date.today()
    checkers = []
    while True:
        print("Starting checks")
        next_check_ts, results = do_stuff(config, checkers, new_day=new_day)
        if results:
            print("\n".join("{}, {}".format(t, t.magnet_link) for t in results))
        time_step = int(next_check_ts - datetime.now().timestamp() - 120)
        print(
            "next check will be at {}, waiting {} seconds".format(
                str_from_ts(next_check_ts), time_step
            )
        )
        sleep(time_step)
        new_day = (base_today != date.today())
        if new_day:
            base_today = date.today()


if __name__ == "__main__":
    try:
        main(read_config(), new_day=True)
    except KeyboardInterrupt:
        print("Exiting...")
        exit(1)
