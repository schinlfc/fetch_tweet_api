#!/usr/bin/env python3
"""Fetch tweets and write to tweets.json"""


import tweepy
import datetime
import json
import time
from interval import Interval
import util
from tqdm import tqdm
import pandas as pd
import itertools
import random
# import clean_tweets


config = json.load(open('config.json', 'rb'))
bearer_token = config['bearer_token']
client = tweepy.Client(bearer_token, wait_on_rate_limit=True)

changeover_dates = [
    #datetime.datetime(2014, 3, 9),
    #datetime.datetime(2014, 11, 2),
    #datetime.datetime(2015, 3, 8),
    #datetime.datetime(2015, 11, 1),
    #datetime.datetime(2016, 3, 13),
    #datetime.datetime(2016, 11, 6),
    #datetime.datetime(2017, 3, 12),
    #datetime.datetime(2017, 11, 5),
    #datetime.datetime(2018, 3, 11),
    #datetime.datetime(2018, 11, 4),
    #datetime.datetime(2019, 3, 10),
    #datetime.datetime(2019, 11, 3),
    # 2020 will be excluded from most analyses, but fetch it
    # anyway for a robustness check later
    #datetime.datetime(2020, 3, 8),
    datetime.datetime(2020, 11, 1),
    #datetime.datetime(2021, 3, 14),
    datetime.datetime(2021, 11, 7),
]

timer = util.AdaptiveSleepTimer()


def retry(timeout, retries):
    def inner(func):
        def inner2(*args, **kwargs):
            i = 0
            last_exception = None
            while i < retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(e)
                    time.sleep(timeout)
                    last_exception = e
                i += 1
            # Ran out of retries.
            raise last_exception
        return inner2
    return inner


@retry(timeout=10, retries=3)
def fetch_tweets(interval):
    print(f'Fetching {interval}')
    cursor = tweepy.Paginator(client.search_all_tweets,
                              query='-is:retweet lang:en has:geo place_country:US',
                              user_fields=['username', 'public_metrics', 'description', 'location'],
                              tweet_fields=['created_at', 'geo', 'public_metrics', 'text'],
                              place_fields=['id', 'geo', 'name', 'full_name'],
                              expansions=['author_id', 'geo.place_id'],
                              start_time=interval.start(),
                              end_time=interval.end(),
                              max_results=500, limit=10)
    tweets = []
    places = []
    for response in cursor:
        user_dict = {}
        place_dict = {}
        for user in response.includes['users']:
            user_dict[user.id] = user.data
        for place in response.includes['places']:
            place_dict[place.id] = place.data
        places.extend(place_dict.values())
        for tweet in response.data:
            try:
                geo = tweet.geo
                if geo is None:
                    print(f'warn - tweet {tweet.id} missing geo info')
                    continue
                place = place_dict[geo['place_id']]
            except KeyError:
                try:
                    place_id = tweet.geo['place_id']
                except KeyError:
                    place_id = 'UNKNOWN'
                print(f'warn - place {place_id} not included in reply')
                continue
            tweets.append({
                'user': user_dict[tweet.author_id],
                'place': place,
                'tweet': tweet.data,
            })

        # There is a 1 RPS limit on this endpoint - see
        # https://tinyurl.com/2p868yn6
        time.sleep(1)
    return tweets, places


def write_tweets(tweets, file_handle):
    for tweet in tweets:
        json.dump(tweet, file_handle)
        file_handle.write('\n')
    # print(f'wrote {len(tweets)} tweets')


def get_interval():
    # Pick a random DST changeover, either spring forward or fall back,
    # create an interval spanning 28 days forward and back, and pick a
    # random 5 minute interval within those 56 days.
    random_transition = random.choice(changeover_dates)
    start = random_transition - datetime.timedelta(days=28)
    end = random_transition + datetime.timedelta(days=28)
    duration = datetime.timedelta(seconds=300)
    return Interval.pick_random_time_interval(start, end, duration)


def get_under_represented_days(remaining_usage):
    # Get under represented days
    # Get number of tweets per day
    engine = util.create_engine()
    with engine.connect() as con:
        tweet_time_summary = pd.read_sql_table('tweet_time_summary', con=con)
        tweet_time_summary = tweet_time_summary.set_index('date_day')
    engine.dispose()
    # Add zeros for days we were supposed to fetch but didn't
    days_to_fetch = []
    for transition in changeover_dates:
        for i in range(-28, 28):
            offset = datetime.timedelta(days=i)
            days_to_fetch.append((transition + offset).strftime('%Y-%m-%d'))
    tweet_time_summary = pd.DataFrame(index=days_to_fetch) \
        .join(tweet_time_summary, how='left') \
        .fillna(0) \
        .astype(int)

    # See how many tweets we could get for the days with the least amount of tweets
    last_count = None
    for count in itertools.count(0, 1000):
        days_under_threshold = tweet_time_summary[tweet_time_summary['cnt'] <= count]
        usage_required = days_under_threshold.size * count - days_under_threshold['cnt'].sum()
        print(f'Raising {days_under_threshold.size} days to {count} requires {usage_required}')
        if usage_required > remaining_usage:
            break
        last_count = count
    assert last_count is not None, "Can't fetch any tweets, too little usage remaining"
    # Turn the expected count of tweets into a probability distribution. Dates with counts
    # close to the expected number will get a low probability, dates with no current tweets
    # will get a high probability
    tweet_time_summary = tweet_time_summary[tweet_time_summary['cnt'] <= last_count].copy()
    tweet_time_summary['shortage'] = last_count - tweet_time_summary['cnt']
    tweet_time_summary['probability'] = tweet_time_summary['shortage'] / tweet_time_summary['shortage'].sum()
    tweet_fetch_probability = tweet_time_summary[['probability']]
    return tweet_fetch_probability


def get_interval_catchup(tweet_fetch_probability):
    # Weighting by the probability distribution, pick a random day
    day = tweet_fetch_probability.sample(1, weights=tweet_fetch_probability['probability']).index[0]
    day = datetime.datetime.strptime(day, '%Y-%m-%d')
    # Within that day, pick a five minute interval at random
    start_of_day = day
    end_of_day = day + datetime.timedelta(days=1)
    duration = datetime.timedelta(seconds=300)
    return Interval.pick_random_time_interval(start_of_day, end_of_day, duration)


def main():
    with open('tweets.json', 'at') as tweets_fh:
        usage_start, total_usage = util.get_usage()
        usage_remaining = max(0, total_usage - usage_start)
        tweets_to_fetch = 7_637_707  # usage_remaining
        tweets_fetched = 0
        # engine = util.create_engine()
        #tweet_fetch_probability = get_under_represented_days(usage_remaining)
        with tqdm(total=tweets_to_fetch) as pbar:
            while tweets_fetched < tweets_to_fetch - 10_000:
                #interval = get_interval_catchup(tweet_fetch_probability)
                interval = get_interval()
                tweets, places = fetch_tweets(interval)
                write_tweets(tweets, tweets_fh)
                # with engine.connect() as con:
                #     clean_tweets.clean_tweets_incremental(tweets, places, con)
                tweets_fetched += len(tweets)
                pbar.update(len(tweets))
        print(f'Fetched {tweets_fetched} in total')
        usage_end, total_usage = util.get_usage()
        print(f'Usage consumed: {usage_end - usage_start}, '
              f'currently {usage_end} / {total_usage}')


if __name__ == '__main__':
    main()
