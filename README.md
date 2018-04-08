# Circle of Trust Leaderboard

This repository contains the implementation of my
[leaderboard](https://redd.it/89wny7) for Reddit's April Fool's 2018
social experiment
["Circle of Trust"](https://www.reddit.com/r/CircleofTrust/), which
ran from Monday, April 2, to Friday, April 6.

Final top 1000 leaderboards (note that caveats with respect to the
completeness of my data apply to an increasing degree past the top 50
or so):
* [Active circles](leaderboards/active.md)
* [Betrayed circles](leaderboards/betrayed.md)
* [Users in the most circles](leaderboards/most-circles.md)

My complete data set is available in [all-data.json.gz](all-data.json.gz).

The code (`circle.py`) is a bit messy (and lacking in documentation).
Sorry about that; I was rushing to try to minimize the amount of data
missed.

## What was Circle of Trust?

Each user had the ability to create a password-protected circle. By
sharing the password, other users could choose to "join" the circle,
increasing its size, or "betray" it instead, preventing it from
growing further. Reddit implemented this as a subreddit, with each
circle a post in the subreddit and the join and betray operations
implemented through special rules for voting.

Each participating user was tagged ("flaired") with the number of
people who had joined the user's circle, the number of circles the
user had joined themselves, and an icon designating if the user had
ever betrayed a circle. For example, "7, 20 ∅" indicated the user (in
this case, Reddit administrator [/u/mjmayank](https://redd.it/897g30))
had 7 people in their circle (the first number), had joined 20 circles
(the second number), and had betrayed at least one circle (the symbol
at the end).

The tag was visible on each comment the user made in the subreddit, as
well as on the user's circle. Although the tag indicated if the user
had betrayed any other circle, it did *not* indicate if the user's own
circle had been betrayed; that data was only available by observing
the circle directly.

## How does this script work?

The Reddit server had significant difficulties keeping listings like
/r/CircleofTrust/top properly sorted. This required me to collect data
on many "largish" circles and then sort it after the fact to determine
the true leaderboard. Data was gathered by watching the following
listings:

* [hot](https://www.reddit.com/r/CircleofTrust/)
  (200 circles)
* [rising](https://www.reddit.com/r/CircleofTrust/rising/)
  (200 circles)
* [top from all time](https://www.reddit.com/r/CircleofTrust/top/?t=all)
  (200 circles)
* [top from past 24 hours](https://www.reddit.com/r/CircleofTrust/top/?t=day)
  (200 circles)
* [top posts matching `flair:betrayed`](https://www.reddit.com/r/CircleofTrust/search?q=flair%3Abetrayed&restrict_sr=on&sort=top)
  (500 circles)
* [top posts matching `NOT flair:betrayed`](https://www.reddit.com/r/CircleofTrust/search?q=NOT+flair%3Abetrayed&restrict_sr=on&sort=top)
  (500 circles)
* [all comments](https://www.reddit.com/r/CircleofTrust/comments/)
  (continuous stream, handled differently)

Each iteration of the script proceeded through the following steps:

1. Observe one of the listings of circles, updating statistics and
   betrayed status for each user
2. Audit several of the top users from each leaderboard who had not
   been audited recently (an audit interval of one hour, later reduced
   to 30 minutes)
3. Receive the next batch(es) of comments from the "all comments"
   stream and update user statistics *except* if their own circle has
   been betrayed
4. Update the posted leaderboard

Each iteration took about 100 seconds on a Raspberry Pi (v1) Model B+.

An "audit" consists of requesting a specific user's statistics. This
was performed by accessing the user's circle URL and retrieving the
data both directly from the embedded circle and also from the
subreddit post associated with it. This was considered authoritative,
due to its completeness (it includes all collected statistics) and
reliability (it was slightly better-insulated from inconsistencies,
such as processing delays, in the Reddit servers).

Charts of statistics over time were produced using the
[Google Image Charts API](https://developers.google.com/chart/image/),
which I was pleased to see still works well six years after it was
deprecated. (Although the length of the URLs made it a bit tricky to
fit three 25-member leaderboards into Reddit's 40kb size limit on self
posts.)

## Running the code

Try this to get started by producing your own copy of the leaderboard:

    cp config.json config.json.example
    sqlite3 circle.db < schema.txt
    zcat all-data.json.gz | python circle.py import
    python circle.py leaderboard

(This example assumes you are on a UNIX-like system and have the
`sqlite3` command-line tool and required Python modules (PyTZ and
PRAW) available. Use the `--help` command-line option to see available
command-line options for this and other subcommands.

## Inaccuracies and limitations

Despite my best attempts, limitations on the available data and the
feasible data collection strategies have introduced some inaccuracies.

### The "joined most circles" leaderboard and data from the comment stream

The "all comments" stream provided data from a broad range of users,
not only those with popular circles. This allowed me to build the
leaderboard of users who had joined the most circles, though not with
perfect accuracy. The accuracy of that leaderboard was compromised by
two bugs, which were not completely resolved until the morning of
April 5:

1. I did not initially implement a routine to audit the leaderboard,
   so users who were not actively commenting would not have their
   scores updated. This was a particular problem for the overnight
   hours, because there would be no observations of their scores
   falling overnight.

2. Once I implemented such an audit routine, I accidentally left an
   exclusion for users whose own circles had been betrayed prior to
   their last audit. While this optimization is useful for largest
   circle leaderboards, this resulted in a second day of inadequate
   data collection for the "joined most circles" leaderboard.

In order to limit the overall amount of data collected, I did not save
any observations from the comment stream from users with fewer than 5
members of their circle or 5 circles joined.

### Circle age and betrayal times

The age of a circle is also subject to some inaccuracy. While the time
at which a circle was created is available, the time at which it was
betrayed is not. I recorded instead the time at which the circle was
*first observed* to be betrayed. For most circles that appeared on the
leaderboard, this should be accurate to within a few minutes. For
other circles on or near the leaderboard, it should be delayed by no
longer than the audit interval. However, because I did not keep track
of when the circle was last observed to be unbetrayed, it is not
possible to pinpoint the time precisely.

Because I was able to monitor the circle's size (but not whether it
was betrayed or not) via the comment stream, the circle could have
been betrayed even before the first observation of the circle at that
size. Thus, the time range when the betrayal could have happened
extends from after the most recent observation with a *smaller* circle
size (since someone was able to join later) to before it was observed
to be betrayed.

### Other notes

An observation was only recorded if it differed from the previous
observation. This means it is not possible to determine the exact time
window when a change in the data occurred. This is true both for
betrayal times, as well as for changes in a user's statistics (for
example, at what time they betrayed their first circle).

A improved design of the script would record a "last seen active" time
for each circle, and a "last valid" time for each observation (so that
each observation would be recorded for a time range, rather than the
single time point when it was first seen).

## The JSON data dump

The data is an object with usernames as keys. For each user there is an object with the following fields:

* `audited` is the time the user was last audited
* `observations` is an array of observations of the user's statistics,
  sorted by time, where each observation is an array of four fields:
  1. The time of the observation
  2. The number of users in the user's own circle
  3. The number of circles the user has joined
  4. `true` if the user has previously betrayed a circle, `false` otherwise

A few users (48) had their user flair turned off for some or all of
the time. For affected observations, the third and fourth fields will
be `null`, and the second field will be `null` unless it was the
observation was produced by an audit.

The user object has additional fields describing the user's circle
(which will all be `null` if the user was not observed to have a circle):

* `created` is the time the circle was created
* `betrayed` is the time the circle was first observed to be betrayed,
  or `null` if was never betrayed.
* `id` is the post ID of the circle
* `title` is the name of the circle

Note that the time given in `betrayed` field is not the time the
circle was betrayed, but rather the time when the circle was first
observed to be betrayed (see "circle age and betrayal times", above).

All times are stored in Unix time.
