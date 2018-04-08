#!/usr/bin/env python2

# Add local lib directory (for praw)
import sys
import os.path
LIBDIR = os.path.join(os.path.dirname(__file__), 'lib')
if os.path.exists(LIBDIR):
    sys.path.append(LIBDIR)

import argparse
import datetime
import json
import logging
import re
import sqlite3
import time
import urllib
import urllib2
from HTMLParser import HTMLParser
from collections import namedtuple

import pytz
import praw
import prawcore

# Initialize logging
logging.basicConfig(
    format='[%(asctime)s] %(levelname)s %(name)s: %(message)s',
    level=logging.INFO
)

# Load configuration
with open('config.json') as f:
    CONFIG = json.load(f)
CONFIG['anonymize'] = set(x.lower() for x in CONFIG.get('anonymize', []))

# Open database
db = sqlite3.connect(CONFIG["dbfile"])
c = db.cursor()

USER_AGENT = CONFIG["user_agent"]

CIRCLE_RESET_TIME = 1522686780  # 2 April 2018 09:33 PDT
CIRCLE_EARLIEST_TIME = 1522674000   # 2 April 2018 06:00:00 PDT
CIRCLE_MONDAY_MIDNIGHT = 1522652400 # 2 April 2018 00:00:00 PDT
CIRCLE_ENDED = 1523032041           # 6 April 2018 09:27:21 PDT

def circle_now():
    if CIRCLE_ENDED:
        return CIRCLE_ENDED
    else:
        return time.time()

ONE_HOUR = 3600
ONE_DAY = 24*ONE_HOUR
HOURS_THRESHOLD = 2*ONE_HOUR
SNAP_ORIGIN = 30*60
DISABLE_PLOTS = False
PLOT_WIDTH = 500
PLOT_NPOINTS = 100               # 100
PLOT_HEIGHT = 175

SUBREDDIT = 'CircleofTrust'     # Circle subreddit
SAFE_CIRCLE = 'nandhp'          # Known existing circle
DEFAULT_STALENESS = 30*60       # Default staleness for daemon audits

REDDIT_GAP = 1                  # Minimum seconds between API requests
AUDIT_INTERVAL = REDDIT_GAP # 4
AUDIT_INTERVAL_DIVISIONS = 1
FASTEST_POSSIBLE = False

# &#x2205; (betray, empty set), &#x2300; (diameter), &#xd8; (O with stroke)
BETRAY_SYMBOL = '&#8709;' #'&#x2205;'

# &#x2609; (sun), &#x298; (latin bilabial click), ...
JOIN_SYMBOL = '&#9737;' #'&#x2609;'

# +&#x20dd; (combining enclosing circle), &#x25f5; (bottom-left) &#x238a; (circled triangle down)
USERS_SYMBOL = '&#9098;' #'&#x238a;';
# http://shapecatcher.com/

######################################################################
# DATABASE UPDATES
######################################################################

UserStats = namedtuple('UserStats', ('followers', 'following', 'betrayer'))
def parse_user_flair(flair):
    if not flair:
        return None
    flair = [int(x.strip(','), 10) if i < 2 else int(bool(x))
             for i, x in enumerate(flair.split(' '))]
    if len(flair) == 2:
        flair.append('')
    return UserStats(*flair)

def _is_betrayed(post_flair):
    if not post_flair:
        return 0
    if 'Betrayed' in post_flair:
        return 1
    raise ValueError("post flair: %s" % (repr(post_flair),))

def observe_circle(postid, author, title, created, betrayed, audited=None):
    # Update circle definition
    c.execute('SELECT id, betrayed FROM circle WHERE id=?', (postid,))
    row = c.fetchone()
    n = 0
    if not row and author:
        c.execute('SELECT id, betrayed FROM circle WHERE author=?', (author,))
        row = c.fetchone()
        if row:
            # Upgrade a previously-missing circle
            assert row[0] is None
            c.execute('UPDATE circle SET id=?, title=?, created=? WHERE author=?',
                      (postid, title, created, author))
            row = (postid,) + row[1:]
            n = 1
    if not row:
        if not author:
            return 0
        c.execute('INSERT INTO circle(id, author, title, created, betrayed, ' +
                  'audited) VALUES(?, ?, ?, ?, ?, ?)',
                  (postid, author, title, created, betrayed, audited))
        n = 1
    else:
        if audited is not None:
            c.execute('UPDATE circle SET audited=? WHERE id=?',
                      (audited, postid))
            n = 1
        if betrayed and not row[1]:
            c.execute('UPDATE circle SET betrayed=? WHERE id=?',
                      (betrayed, postid))
            n = 1
    return n

def observe_missing_circle(author, betrayed=None, audited=None):
    if not author:
        return 0
    # Update circle definition
    c.execute('SELECT author, betrayed FROM circle WHERE author=?',
              (author,))
    row = c.fetchone()
    n = 0
    if not row:
        c.execute('INSERT INTO circle(id, author, title, created, betrayed, ' +
                  'audited) VALUES(NULL, ?, NULL, NULL, ?, ?)',
                  (author, betrayed, audited))
        n = 1
    else:
        if audited is not None:
            c.execute('UPDATE circle SET audited=? WHERE author=?',
                      (audited, row[0]))
            n = 1
        if betrayed and not row[1]:
            c.execute('UPDATE circle SET betrayed=? WHERE author=?',
                      (betrayed, row[0]))
            n = 1
    return n

def observe_circle_post(post, now=None):
    if '/circle/embed/' not in post.url:
        return 0
    if now is None:
        now = time.time()
    if post.created_utc < CIRCLE_RESET_TIME or now < CIRCLE_RESET_TIME:
        return 0
    betrayed = now if _is_betrayed(post.link_flair_text) else None

    return observe_circle(post.id, post.author.name if post.author else None,
                          post.title, post.created_utc, betrayed)

def observe_user(now, author, stats):
    # Check nearest sample in each direction
    for clause in 'time<=? ORDER BY time DESC', 'time>? ORDER BY time ASC':
        # Also order by rowid (newest first) for tie-breaking
        c.execute(('SELECT followers, following, betrayer, time FROM user ' +
                   'WHERE author=? AND %s, rowid DESC LIMIT 1') % (clause,),
                  (author, now))
        for row in c.fetchall():
            # Skip adding this observation if there is a matching neighbor
            obsnow = row[-1]
            #print author, rowcmp[:3], row[:3], now, obsnow
            if abs(now-obsnow) < 1:
                return 0        # Skip: observation within one second
            assert len(stats) == len(row)-1
            if all(x == y for x, y in zip(stats, row[:3])):
                return 0   # Skip: no change
            #if flairitems[0] < row[0]:
            #    print "WARNING regressing observation"
    #print 'ADDING', author

    # Store observation
    c.execute('INSERT INTO user' +
              '(time, author, followers, following, betrayer) ' +
              'VALUES(?, ?, ?, ?, ?)',
              (now, author, stats.followers, stats.following, stats.betrayer))
    return 1

def observe_user_post(thing, now=None, baseline=None):
    if now is None:
        now = time.time()
    elif now < CIRCLE_RESET_TIME:
        return 0

    stats = parse_user_flair(thing.author_flair_text)
    if not stats:
        return 0
    if baseline is not None:
        if stats[0] < baseline and stats[1] < baseline:
            return 0

    return observe_user(now, thing.author.name, stats)

######################################################################
# OBSERVE/INGEST SUBCOMMANDS
######################################################################

_reddit = None
def get_reddit():
    global _reddit
    if not _reddit:
        _reddit = praw.Reddit(user_agent=USER_AGENT,
                              client_id=CONFIG["client_id"],
                              client_secret=CONFIG["client_secret"],
                              username=CONFIG['username'],
                              password=CONFIG['password'])
    return _reddit

def get_subreddit(reddit=None):
    if not reddit:
        reddit = get_reddit()
    return reddit.subreddit(SUBREDDIT)

def run_daemon(args):
    """Daemon mode to continuously monitor circles and users, and
    (optionally) regularly update the leaderboard.
    """

    observation_cycle_len = 10
    iteration = int(time.time()/60) % observation_cycle_len
    comment_stream = do_observe_comments(args, pause_after=0)
    comment_stream.next()

    while True:
        iteration_start = time.time()
        logging.info('Starting iteration %d', iteration)
        # Observe top posts (circles)
        if not args.no_observe:
            try:
                if iteration in (2, 8):
                    do_observe(args, 'NOT flair:betrayed')
                elif iteration == 5:
                    do_observe(args, 'flair:betrayed')
                elif iteration == 4:
                    do_observe(args, '/rising')
                elif iteration in (0, 6):
                    do_observe(args, '/hot')
                elif iteration in (3, 9):
                    do_observe(args, '/top/day')
                else:               # 1,7
                    do_observe(args, '/top')
            except Exception as e:
                logging.exception(e)
                logging.warn("Sleeping due to exception in observe")
                time.sleep(60)

        audittimes = min(4, 60/AUDIT_INTERVAL-1)
        auditdelay = AUDIT_INTERVAL/AUDIT_INTERVAL_DIVISIONS
        if iteration in (0, 2, 4, 6, 8):
            auditquery = -1     # Top users
        else:                   # 1,3,5,7,9
            auditquery = 0      # Top circles
        try:
            for i in range(audittimes):
                do_audit(args, auditquery, DEFAULT_STALENESS,
                         args.audit_density,
                         sleep=0 if args.audit_density == 1 else -1)
                for i in range(AUDIT_INTERVAL_DIVISIONS):
                    next_audit = time.time() + auditdelay
                    comment_stream.send(next_audit-1)
                    # Sleep at least two seconds before next audit
                    if not FASTEST_POSSIBLE:
                        time.sleep(max(next_audit-time.time(), REDDIT_GAP))
        except Exception as e:
            logging.exception(e)
            logging.warn("Sleeping due to exception in audit")
            time.sleep(60)

        # Update leaderboard
        if args.update:
            logging.info("Running leaderboard update")
            try:
                do_leaderboard(args.update)
            except Exception as e:
                logging.exception(e)
                logging.warn("Sleeping due to exception in leaderboard")
                time.sleep(60)
        else:
            logging.info("Skipping leaderboard update")

        # Next iteration
        logging.info('---- Iteration took %d seconds ----',
                     time.time() - iteration_start)
        iteration = (iteration+1) % observation_cycle_len

def save(args, count):
    if args.dry_run:
        logging.info("Would update %d entries (dry-run)", count)
    else:
        db.commit()
        logging.info("Updated %d entries", count)

def do_observe(args, query, do_save=False):
    sr = get_subreddit()

    now = time.time()
    if do_save:
        print time.time()
        print

    if not query:
        query = '/top'
    logging.info("Observing query %s", query)
    if query == '/top/day':
        query = sr.top('day', limit=200)
    elif query == '/top' or query == '/top/all':
        query = sr.top('all', limit=200)
    elif query == '/hot':
        query = sr.hot(limit=200)
    elif query == '/new':
        query = sr.new(limit=200)
    elif query == '/rising':
        query = sr.rising(limit=200)
    else:
        query = sr.search(query, sort='top', limit=500)

    count = 0
    for post in query:
        if do_save:
            print post.author, post.id, post.created_utc
            print post.title.encode('utf-8')
            print post.link_flair_text
            print post.author_flair_text.encode('utf-8') if post.author_flair_text else None
            print
            count += 1
        else:
            n = 0
            n += observe_circle_post(post, now)
            n += observe_user_post(post, now)
            if n > 0:
                count += 1
    save(args, count)

def run_observe(args):
    """Observe circles (posts) matching given listing or search query."""

    return do_observe(args, args.query, do_save=args.save)

def do_observe_comments(args, pause_after=None):
    sr = get_subreddit()
    stream = sr.stream.comments(pause_after=pause_after)
    deadline = 0 if pause_after is not None else None

    def _handle_comment(comment, now):
        n = 0
        if comment.author and comment.author.name:
            n += observe_missing_circle(comment.author.name, None)
        n += observe_user_post(comment, now, baseline=5)
        return 1 if n > 0 else 0

    # Fetch 200 comments to start us off
    logging.info("Initializing comment stream")
    now = time.time()
    seen, count = 0, 0
    for comment in sr.comments(limit=200):
        seen += 1
        count += _handle_comment(comment, now)
    logging.info("Seen %d comments (initialization)", seen)
    save(args, count)

    while True:
        # If we are given a deadline, pause each iteration to return
        # to the caller
        if deadline is not None: # and time.time() >= deadline: ...continue
            deadline = yield None
            assert deadline is not None

        now = time.time()

        logging.info("Observing comments")
        seen, count, iters = 0, 0, 0
        comment = True
        # Observe comments until we run out of data or hit the deadline
        while comment and (deadline is None or now < deadline or iters == 0):
            for _ in range(100):
                comment = stream.next()
                if not comment:
                    break
                seen += 1
                count += _handle_comment(comment, now)
            now = time.time()
            if deadline is None:
                logging.info("Seen %d comments (continuing)", seen)
                save(args, count)
                seen, count = 0, 0
            iters += 1
        # Save all of the comments we saw this time
        logging.info("Seen %d comments%s", seen,
                     " (out of time)" if comment else " (out of data)")
        save(args, count)

def run_observe_comments(args):
    """Continuously observe the /r/CircleofTrust comment stream."""

    do_observe_comments(args, pause_after=None).next()

def find_user_comment(username, reddit=None, limit=200):
    if not reddit:
        reddit = get_reddit()
    user = reddit.redditor(username)
    matchsr = SUBREDDIT.lower()
    try:
        for comment in user.comments.new(limit=limit):
            if comment.subreddit.display_name.lower() == matchsr:
                return comment
            if comment.created_utc < CIRCLE_EARLIEST_TIME:
                return None
    except prawcore.exceptions.NotFound:
        # User is deleted, shadowbanned, etc.
        logging.error("Got 404 on comments for %s", username)
    return None

FakeAuthor = namedtuple('FakeAuthor', ('name',))
FakeThing = namedtuple('FakeThing', ('id', 'title', 'url', 'created_utc',
                                     'link_flair_text', 'author',
                                     'author_flair_text'))
def run_ingest(args):
    """Ingest data saved by interim monitoring scripts."""

    skip_things = set(('890d6q', '897byw'))
    count = 0
    for fn in args.files:
        lines = [l.rstrip('\r\n').decode('utf-8')
                 for l in open(fn).readlines()]
        if 'praw is outdated' in lines[0]:
            lines.pop(0)
        now = float(lines.pop(0).strip())
        lines.pop(0)
        while len(lines) > 4:
            #print lines[:5]
            author, postid, created = lines.pop(0).split()
            title = lines.pop(0)
            linkflair = lines.pop(0)
            userflair = lines.pop(0)
            if author == 'None':
                author = None
            if userflair == 'None':
                print "Skipping user without flair: %s" % (author,)
            elif postid not in skip_things:
                author = FakeAuthor(author)
                thing = FakeThing(postid, title, '/circle/embed/',
                                  float(created),
                                  None if linkflair == 'None' else linkflair,
                                  author, userflair)
                n = 0
                n += observe_circle_post(thing, now)
                n += observe_user_post(thing, now)
                if n > 0:
                    count += 1
            else:
                logging.warning("Skipping %s: %s", postid, title)
            while lines and not lines[0]:
                lines.pop(0)
    save(args, count)

CIRCLE_CONFIG_RE = re.compile(r'<script type="text/javascript" id="config">' +
                              r'r\.setup\((.*?)\);?<\/script>')
CIRCLE_USERNAME_RE = re.compile(r'<link rel="canonical" ' +
                                r'href="https://www.reddit.com/user/([^/"]*)/')
CIRCLE_TITLE_RE = re.compile(r'<div class="[^"]*circle-title[^"]*"><a href=' +
                             r'"/r/[^/"]+/comments/([0-9a-zA-Z]+)/[^/"]*/">' +
                             r'([^<>]*)</a></div>')
def get_circle(username):
    url = 'https://www.reddit.com/user/%s/circle/embed.json' % (username,)
    req = urllib2.Request(url, headers={'User-Agent': USER_AGENT})
    resp = urllib2.urlopen(req)
    #print resp.code
    #print resp.headers
    data = resp.read()

    _htmlparser = HTMLParser()

    obj = json.loads(CIRCLE_CONFIG_RE.search(data).group(1))
    match = CIRCLE_TITLE_RE.search(data)
    obj['x_circle_submitted'] = match.group(1)
    obj['x_circle_title'] = _htmlparser.unescape(match.group(2).decode('utf-8'))
    obj['x_username'] = CIRCLE_USERNAME_RE.search(data).group(1)
    return obj

def refresh_circle(username, dry_run=False, verbose=False):
    try:
        obj = get_circle(username)
    except urllib2.HTTPError as exc:
        if exc.code != 404:
            raise
        logging.warning("Got 404 on circle for %s...", username)
        if not get_circle(SAFE_CIRCLE):
            raise
        logging.warning("...but Circle is still working")
        obj = None

    url = 'https://www.reddit.com/comments/%s' % (obj['x_circle_submitted'],) \
          if obj else None

    if verbose:
        if obj:
            print ('%s %s' % (obj['x_circle_title'], url)).encode('utf-8')
            print 'Owner: %s' % (obj['x_username'],)
            print 'Members: %3d (%d outside)' % (obj['circle_num_inside'],
                                                 obj['circle_num_outside'])
            print 'Betrayed: %s' % (obj['circle_is_betrayed'],)
            #print
            #print 'User WS:', obj['user_websocket_url']
            #print 'Circle WS:', obj['circle_websocket_url']
        else:
            print 'No circle for user %s' % (username,)

    now = time.time()
    n = 0

    if not FASTEST_POSSIBLE:
        time.sleep(REDDIT_GAP)      # Wait after fetching circle
    reddit = get_reddit()

    if url:
        assert obj
        post = reddit.submission(url=url)
        assert post.id == obj['x_circle_submitted']
        assert post.author.name == obj['x_username']
        assert post.title == obj['x_circle_title'] # FIXME
        assert obj['circle_is_betrayed'] in (True, False)
        betrayed = now if obj['circle_is_betrayed'] else None
        if not dry_run:
            n += observe_circle(post.id,
                                post.author.name if post.author else None,
                                post.title, post.created_utc, betrayed,
                                audited=now)
    else:
        assert not obj
        if not dry_run:
            # Just list as audited, don't mark as betrayed
            n += observe_missing_circle(username, None, audited=now)
        post = find_user_comment(username, reddit=reddit)
        if not post:
            logging.warning("No circle posts found for user %s", username)
            return 0

    # Patch stats from view results
    stats = parse_user_flair(post.author_flair_text)
    stats = UserStats(followers=obj['circle_num_inside'] if obj else \
                      stats.followers if stats else None,
                      following=stats.following if stats else None,
                      betrayer=stats.betrayer if stats else None)
    if verbose:
        print
        print "Followers: %s" % (stats.followers,)
        print "Following: %s" % (stats.following,)
        print "Betrayer: %s" % (bool(stats.betrayer),)
    n += observe_user(now, post.author.name, stats)
    return 1 if n > 0 else 0

def run_view(args):
    """Fetch and display information about a given user and their circle."""

    count = 0
    for username in args.username:
        count += refresh_circle(username, dry_run=args.dry_run, verbose=True)
    return save(args, count)

def do_audit(args, query_type, staleness, total=10, sleep=-1):
    if sleep == -1:
        sleep = AUDIT_INTERVAL
    if query_type == -1:
        leaders = get_following_leaders(total, stale_audit=staleness)
    elif query_type in (0, 1, 2):
        betrayed_query = (None, False, True)[query_type]
        leaders = get_leaders(total, betrayed=betrayed_query,
                              stale_audit=staleness)
    else:
        raise ValueError

    count = 0
    for row in leaders:
        author = row[0]
        logging.info("Auditing %s (query type %d)", author, query_type)
        n = refresh_circle(author, dry_run=False, verbose=False)
        count += n
        save(args, n)
        if sleep:
            time.sleep(sleep)
    if total > 1:
        logging.info("Total updates found: %d", count)
    return count, len(leaders)

def run_audit(args):
    """Audit the next batch of users from the given leaderboard."""

    return do_audit(args, args.query_type, args.staleness, total=args.count)

######################################################################
# LEADERBOARD GENERATION
######################################################################

# API Docs: https://developers.google.com/chart/image/
CHART_CODING = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
CHART_BASEURL = 'https://chart.googleapis.com/chart?'
#CHART_BASEURL = 'https://www.google.com/chart?' # Broken: referer filtering

def _chart_encode(val, yrange):
    val = int(round((len(CHART_CODING)-1)*val/yrange))
    return CHART_CODING[val]

def format_lifetime(dur):
    if dur < HOURS_THRESHOLD:
        return '%dm' % (dur/60,)
    if dur < ONE_DAY:
        return '%dh%dm' % (int(dur/ONE_HOUR), (dur/60) % 60)
    return '%dd%dh' % (int(dur/ONE_DAY), (dur/ONE_HOUR) % 24)

def _urlencode(query):
    _quote = lambda s: urllib.quote_plus(s, ',:*[]')
    return '&'.join(_quote(k) + '=' + _quote(v) for k, v in query.items())

def make_plot(points, title, created=None, betrayed=None, audited=None,
              with_timestamp=True, title_prefix=None, force_snap=False,
              betrayer=None, now=None, verbose=False):
    if DISABLE_PLOTS:
        return None
    if now is None:
        now = time.time()
    start = min(p[0] for p in points)
    if created and (force_snap or start-created < SNAP_ORIGIN):
        start = created         # Snap axis to 0 if nearby
    if not created:
        created = start
    if not title_prefix:
        title_prefix = ''
    end = betrayed if betrayed else now #max(p[0] for p in points)
    yrange = max(p[1] for p in points)
    if not yrange:
        return None

    # Reduced from 128 to better fit in 40K
    width, npoints, height = PLOT_WIDTH, PLOT_NPOINTS, PLOT_HEIGHT
    # npoints = int((end-start)/60/2) for 2-minute points
    dx = (end-start)/(npoints-1)

    p = 0
    chd = 's:'
    dchar = yrange/(len(CHART_CODING)-1)
    for i in range(npoints):
        x = int(start + i*dx)
        while p < len(points) and points[p][0] < x:
            p += 1
        # if p < len(points):
        #     print int(x), time.ctime(x), points[p], time.ctime(points[p][0]), chd[-3:]
        if p >= len(points) and ((audited is not None and audited > x) or
                                 (betrayed is not None and betrayed > x)):
            # Even though we have no further datapoints, we know it
            # can't have changed prior to our most recent audit
            # (because if it had, there would be another data point).
            assert len(chd) > 2
            chd += chd[-1]
        elif p >= len(points):
            chd += '_'
        elif points[p][1] is None:
            chd += '_'
        elif (p == 0 or (p > 0 and
                         points[p][0]-points[p-1][0] > 2*ONE_HOUR and
                         abs(points[p][1]-points[p-1][1]) > 3*dchar)) and \
             points[p][0]-x > dx:
            # For the initial gap and any gap longer than two hours,
            # show a break in the line. But don't show a break for a
            # long gap if there was only a small change. (Assume inactivity)
            chd += '_'
        else:
            chd += _chart_encode(points[p][1], yrange)

    # Determine dates. NOT GOOD PRACTICE, but I don't expect issues
    # with time zones or daylight savings or leap seconds while Circle
    # is running.
    start_date = (start-CIRCLE_MONDAY_MIDNIGHT)*1./ONE_DAY + 2
    end_date = (end-CIRCLE_MONDAY_MIDNIGHT)*1./ONE_DAY + 2
    dates = []
    ticks_per_day = 4 #if end_date-start_date <= 2 else 2
    for i in range(int(start_date*ticks_per_day),
                   int(end_date*ticks_per_day)+1):
        d = i*1./ticks_per_day
        if start_date <= d and d <= end_date:
            day, hour = int(d), (d*24)%24
            hour12 = 12 if hour == 0 or hour == 12 else hour%12
            ampm = 'A' if hour < 12 else 'P'
            if not dates:
                #dates.append((d, '%d-Apr %02d:00%8s' % (day, hour, '')))
                dates.append((d, '%d-Apr %d%s%7s' % (day, hour12, ampm, '')))
            elif hour:
                #dates.append((d, '%02d:00' % (hour,)))
                dates.append((d, '%d%s' % (hour12, ampm)))
            else:
            #dates.append((d, '%d-Apr %02d:00' % (day, hour)))
                dates.append((d, '%d-Apr' % (day,)))

    start -= created
    end -= created

    # Determine axis scale
    if end > HOURS_THRESHOLD:
        xfactor, xunit = ONE_HOUR, 'h' # Hours
    else:
        xfactor, xunit = 60, 'm' # Minutes

    chart = {
        'cht': 'lc',                      # Chart type: line chart
        'chs': '%dx%d' % (width, height), # Chart size
        'chd': chd,             # Chart data, encoded as above
        # Chart title
        'chtt': title_prefix + ('%s [%s]' % (title, format_lifetime(end))
                                if with_timestamp else title),
        # Title is red if betrayed (size) or a betrayer (circles)
        'chts': 'ff0000' if (betrayed or betrayer) else '000000',
        # Chart axes: x-axis (time), y-axis, x-axis (wall clock)
        'chxt': 'x,y,x',
        # Axis styles
        'chxs': ('0N*f*' + xunit + ',000000,12,0,lt|' + # X-axis format
                 '1,000000,12,1,lt|' + # Y-axis format
                 '2,333333,10,0,t'), # Clock-axis format
        # Axis ranges
        'chxr': '0,%f,%f|1,0,%f|2,%.3f,%.3f' % (
            start/xfactor, end/xfactor, yrange, start_date, end_date),
        # Axis labels (clock axis)
        'chxtc': '2,-%d' % (height,),
        'chxp': '2,' + ','.join(str(x[0]) for x in dates),
        'chxl': '2:|' + '|'.join(x[1] for x in dates),
    }

    # Mark betrayer
    if betrayer:
        print betrayer, betrayer-created, betrayer-created-start, end-start
        betrayer -= created
        betraypoint = (betrayer-start)*1./(end-start)
        chart['chm'] = 'R,dd0000,0,%.3f,%.3f' % (betraypoint,
                                                 betraypoint+0.004)

    if verbose:
        print '\n'.join('='.join(x) for x in chart.items())

    return CHART_BASEURL + _urlencode(chart)

def do_plot(author, min_points=None, verbose=False, end=None):
    query = 'SELECT title, created, betrayed, audited FROM circle ' + \
            'WHERE author=?'
    c.execute(query, (author,))
    result = c.fetchone()
    if not result:
        return None
    title, created, betrayed, audited = result
    query = 'SELECT time, followers FROM user WHERE author=? ' + \
            'ORDER BY time ASC'
    c.execute(query, (author,))
    points = c.fetchall()
    if min_points and len(points) < min_points:
        return None
    return make_plot(points, author, created, betrayed, audited,
                     title_prefix='Circle: ', now=end, verbose=verbose)

def do_plot_following(author, min_points=None, verbose=False, end=None):
    query = 'SELECT time, following, betrayer FROM user WHERE author=? ' + \
            'ORDER BY time ASC'
    c.execute(query, (author,))
    points = c.fetchall()
    if min_points and len(points) < min_points:
        return None
    betrayer = None
    if points[-1][2]:
        for i, r in enumerate(points):
            if r[2]:
                betrayer = r[0] if i == 0 else (points[i-1][0]+points[i][0])/2
                break
    return make_plot(points, author, created=CIRCLE_RESET_TIME,
                     #force_snap=True,
                     title_prefix='Joined: ', with_timestamp=False,
                     betrayer=betrayer, now=end, verbose=verbose)

def run_plot(args):
    """Produce plots for a given user."""

    end_time = CIRCLE_ENDED if CIRCLE_ENDED else None
    print "=> Followers:", do_plot(args.username, verbose=True, end=end_time)
    print "=> Following:", do_plot_following(args.username, verbose=True,
                                             end=end_time)

# From snudown, &()- removed; only escape . after a digit.
MARKDOWN_SPECIAL_RE = re.compile(r'[\\`*_{}\[\]#+!:|<>/^~]|(?<=\d)\.',
                                 flags=re.UNICODE)
def escape_markdown(data):
    """Escape characters with special meaning in Markdown."""
    def _replacement(match):
        """Backslash-escape all characters matching MARKDOWN_SPECIAL_RE."""
        return '\\' + match.group(0)
    return MARKDOWN_SPECIAL_RE.sub(_replacement, data)

LONG_WORD_RE = re.compile(r'\S{31}[^\s\\](?=\S{3})', flags=re.UNICODE)
def allow_linebreak(data):
    def _replacement(match):
        return match.group(0) + '&#8203;' # zero-width space
    return LONG_WORD_RE.sub(_replacement, data)

def get_leaders(count, betrayed=None, stale_audit=0, existing_only=False):
    whereclause = []
    if existing_only:
        whereclause.append('id NOT NULL')
    if betrayed is not None:
        betrayclause = 'betrayed'
        if betrayed is True:
            betrayclause += ' != 0 AND betrayed NOT NULL'
        elif betrayed is False:
            betrayclause += ' == 0 OR betrayed IS NULL'
        else:
            raise ValueError
        whereclause.append(betrayclause)

    if stale_audit:
        assert stale_audit >= 1
        #  OR time-audited > %d [[[with stale_audit]] doesn't work for
        #  circles with no changes
        whereclause.append('audited IS NULL OR ' +
                           (('((betrayed IS NULL or betrayed == 0) AND (' +
                             'max(time, audited) < %d)) OR '
                            ) % (circle_now()-stale_audit,)) +
                           '((betrayed IS NOT NULL or betrayed != 0) AND ' +
                           ' audited < betrayed)')

    whereclause = ('WHERE (' + ') AND ('.join(whereclause) + ') ') \
                  if whereclause else ''
    query = 'SELECT user.author, followers, following, betrayer, id, ' + \
            '    title, betrayed, created FROM ' + \
            '(SELECT author, max(time) as maxtime FROM user ' + \
            '    GROUP BY author) AS newest ' + \
            'INNER JOIN user ON user.author == newest.author AND ' + \
            '    user.time == newest.maxtime ' + \
            'INNER JOIN circle ON circle.author == user.author ' + \
            whereclause + 'ORDER BY followers DESC, created ASC LIMIT ?;'

    # In case of a tie, older circles retain higher position, even
    # though one could consider faster growth to be a greater achievement
    c.execute(query, (count,))
    return c.fetchall()

def get_following_leaders(count, stale_audit=0):
    whereclause = []
    if stale_audit:
        assert stale_audit >= 1
        #  OR time-audited > %d [[[with stale_audit]] doesn't work for
        #  circles with no changes
        whereclause.append('audited IS NULL OR ' +
                           ('max(time, audited) < %d ' %
                            ((circle_now()-stale_audit),)))

    whereclause = ('WHERE (' + ') AND ('.join(whereclause) + ') ') \
                  if whereclause else ''
    query = 'SELECT user.author, followers, following, betrayer FROM ' + \
            '(SELECT author, max(time) as maxtime FROM user ' + \
            '    GROUP BY author) AS newest ' + \
            'INNER JOIN user ON user.author == newest.author AND ' + \
            '    user.time == newest.maxtime ' + \
            'INNER JOIN circle ON circle.author == user.author ' + \
            whereclause + 'ORDER BY following DESC, followers DESC LIMIT ?;'
    c.execute(query, (count,))
    return c.fetchall()

def get_max_following(author):
    query = 'SELECT max(following) FROM user WHERE author=?;'
    c.execute(query, (author,))
    row = c.fetchone()
    return row[0] if row else None

def post_permalink(postid):
    return 'https://redd.it/' + postid
    #return '//redd.it/' + postid

def do_leaderboard(update=None, length=None, full_urls=False):
    if length is None:
        length = 25

    now = time.time()
    chart_upload = CONFIG["chart_relay_upload"]
    chart_base = CONFIG["chart_relay_base"]
    charturls = {} if (chart_base and update) else None
    charttitleprefix = {'c': 'Circle: ', 'u': 'Joined: '}

    def _wrap_plot(url, prefix, username):
        if charturls is None or not url:
            return url
        chartid = '%s:%s' % (prefix, username)
        charturls[chartid] = {'title': charttitleprefix[prefix] + username,
                              'url': url}
        return chart_base + chartid

    def _link_user(author, label=None, suffix=None):
        url = 'u/' + author + (suffix if suffix else '')
        if full_urls or suffix or label:
            if not label:
                label = escape_markdown(url)
            if full_urls:
                url = 'https://www.reddit.com/' + url
            return '[%s](%s)' % (label, url)
        else:
            return url

    circle_legend = ("\\#", "Circle", "Size", "Age", "Owner")
    def _render_circles(leaders):
        yield '|'.join(circle_legend)
        yield '|'.join('-' for l in circle_legend)
        for i, row in enumerate(leaders):
            author, followers, following, betrayer, postid, title, \
                betrayed, created = row
            if author.lower() in CONFIG['anonymize']:
                yield '|'.join(
                    (str(i+1), '*Anonymous circle*', str(followers),
                     '&mdash;', '&mdash;')).encode('utf-8')
                continue
            #if not postid:
            #    link = '[*Collecting data...*](/u/%s/circle/)' % (author,)
            #    age = '&mdash;'
            #else:
            link = '[%s](%s)' % \
                   (allow_linebreak(escape_markdown(title)),
                    post_permalink(postid))
            if betrayed:
                end_time = betrayed
            elif CIRCLE_ENDED:
                end_time = CIRCLE_ENDED
            else:
                end_time = now
            age = format_lifetime(end_time-created)
            authorinfo = '&mdash;' if following is None else '%d' % (following,)
            if betrayer:
                authorinfo += ' ' + BETRAY_SYMBOL
            followers = str(followers)
            plot = _wrap_plot(do_plot(author, min_points=10, end=end_time),
                              'c', author)
            if plot:
                followers = '[%s](%s)' % (followers, plot)
            author = '%s (%s)' % (_link_user(author), authorinfo)
            yield '|'.join(
                (str(i+1), link, str(followers), age, author)
            ).encode('utf-8')

    user_legend = ("\\#", "User", BETRAY_SYMBOL, "Joined", "Peak", "Own Circle")
    def _render_users(leaders):
        yield '|'.join(user_legend)
        yield '|'.join('-' for l in user_legend)
        for i, row in enumerate(leaders):
            author, followers, following, betrayer = row
            if author.lower() in CONFIG['anonymize']:
                yield '|'.join((str(i+1), '*Anonymous user*', betrayer,
                                str(following), '&mdash;')).encode('utf-8')
                continue
            if full_urls:
                followers = _link_user(author, followers, '/circle/') \
                            if followers else '&mdash;'
                # '[%s](/u/%s/circle/)' % (followers, author)
            else:
                followers = str(followers) if followers else '&mdash;'
            betrayer = BETRAY_SYMBOL if betrayer else ''
            following = str(following)
            if CIRCLE_ENDED:
                end_time = CIRCLE_ENDED
            else:
                end_time = now
            plot = _wrap_plot(do_plot_following(author, min_points=10,
                                                end=end_time), 'u', author)
            if plot:
                following = '[%s](%s)' % (following, plot)
            peak = get_max_following(author)
            peak = str(peak) if peak is not None else ''
            author = _link_user(author)
            yield '|'.join(
                (str(i+1), author, betrayer, following, peak, followers)
            ).encode('utf-8')

    def _leaderboard_stamp():
        c.execute('SELECT time FROM user ORDER BY time DESC LIMIT 1')
        row = c.fetchone()
        dt = datetime.datetime.fromtimestamp(row[0], pytz.utc)
        dttz = dt.astimezone(pytz.timezone('America/Los_Angeles'))
        dtstr = dttz.strftime('%d %b, %I:%M %p PDT (UTC-7)').lstrip('0')
        if False:
            dtstr = ('[%s](https://www.timeanddate.com/worldclock/' +
                     'fixedtime.html?iso=%s&p1=137)') % \
                     (dtstr, dttz.strftime('%Y%m%dT%H%M'))
        return dtstr

    def _active_circles():
        return list(_render_circles(get_leaders(length, False,
                                                existing_only=True)))

    def _betrayed_circles():
        return list(_render_circles(get_leaders(length, True,
                                                existing_only=True)))

    def _following_users():
        return list(_render_users(get_following_leaders(length)))

    leaderboard = [
        x.rstrip('\r\n') for x in open('postheader.txt').readlines()
    ] + [
        'Last update: %s' % (_leaderboard_stamp(),),
        '',
        '# %s &nbsp; Active circles' % (JOIN_SYMBOL,),
        '',
    ] + _active_circles() + [
        '',
        '# %s &nbsp; Betrayed circles' % (BETRAY_SYMBOL,),
        '',
    ] + _betrayed_circles() + [
        '',
        '# %s &nbsp; Users in the most circles' % (USERS_SYMBOL,),
        '',
    ] + _following_users()

    leaderboard = '\n'.join(leaderboard)
    if update:
        if charturls is not None and not DISABLE_PLOTS:
            assert False
            urllib2.urlopen(chart_upload, data=urllib.urlencode({
                'data': json.dumps(charturls)
            })).read()
        reddit = get_reddit()
        post = reddit.submission(id=update)
        try:
            post.edit(leaderboard)
        except praw.exceptions.APIException as exc:
            if exc.error_type != 'TOO_LONG':
                raise
            logging.error("Leaderboard update failed: length is %d > 40000",
                          len(leaderboard))
    else:
        print leaderboard

def run_leaderboard(args):
    """Generate the leaderboards."""

    return do_leaderboard(args.update, args.length, args.full_urls)

######################################################################
# So long, and thanks for all the fish.
######################################################################

def run_export(args):
    """Export the database to JSON (output via STDOUT)."""

    c.execute('SELECT circle.author, time, followers, following, betrayer, id, title, created, betrayed, audited FROM user INNER JOIN circle ON circle.author=user.author ORDER BY circle.author ASC, time ASC')
    lastauthor = None
    authornum = 0
    def _print_author():
        sys.stdout.write(',\n' if authornum > 0 else '{\n')
        json.dump(lastauthor['author'], sys.stdout)
        del lastauthor['author']
        sys.stdout.write(': ')
        json.dump(lastauthor, sys.stdout, separators=(',', ':'),
                  sort_keys=True)

    for i, row in enumerate(c):
        author, time, followers, following, betrayer, postid, title, \
            created, betrayed, audited = row
        if lastauthor and author != lastauthor['author']:
            _print_author()
            lastauthor = None
            authornum += 1
        if lastauthor is None:
            lastauthor = {
                'author': author,
                'created': int(created) if created else None,
                'betrayed': int(betrayed) if betrayed else None,
                'audited': int(audited) if audited else None,
                'id': postid if postid else None,
                'title': title if title else None,
                'observations': []
            }
        if following is not None:
            assert betrayer is not None
            betrayer = bool(betrayer)
        else:
            assert betrayer is None
        lastauthor['observations'].append((int(time), followers, following,
                                           betrayer))
    else:
        if lastauthor:
            _print_author()
    sys.stdout.write('\n}')

def run_import(args):
    """Import to the database from JSON (input via STDIN)."""

    count = 0
    for user, data in json.load(sys.stdin).items():
        n = observe_circle(data['id'], user, data['title'],
                           data['created'], data['betrayed'], data['audited'])
        assert n == 1
        for observation in data['observations']:
            n += observe_user(observation[0], user,
                              UserStats(*observation[1:]))
        logging.debug("%3d observations for %s", n-1, user)
        count += n-1
    save(args, count)


######################################################################
# MAIN FUNCTION
######################################################################

def main(args):
    # Main argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', '--no-act', action='store_true',
                        help="Dry-run, don't update DB")
    subparsers = parser.add_subparsers(title='subcommands')

    # "daemon" subcommand
    parser_dmn = subparsers.add_parser('daemon',
                                       help=run_daemon.__doc__)
    parser_dmn.add_argument('--update', metavar='POSTID', action='store',
                            help="Update given post ID.") # SAME AS LEADERBOARD
    parser_dmn.add_argument('--no-observe', action='store_true',
                            help="Don't observe post listings.")
    parser_dmn.add_argument('--audit-density', default=1,
                            action='store', type=int,
                            help="Don't observe post listings.")
    parser_dmn.set_defaults(func=run_daemon)


    # "observe" subcommand
    parser_obs = subparsers.add_parser('observe',
                                       help=run_observe.__doc__)
    parser_obs.add_argument('--save', action='store_true',
                            help="Output results for later ingestion")
    parser_obs.add_argument('query', nargs='?',
                            help="Search query (instead of all top)")
    parser_obs.set_defaults(func=run_observe)

    # "observe-comments" subcommand
    parser_obc = subparsers.add_parser('observe-comments',
                                       help=run_observe_comments.__doc__)
    parser_obc.set_defaults(func=run_observe_comments)

    # "ingest" subcommand
    parser_ing = subparsers.add_parser('ingest',
                                       help=run_ingest.__doc__)
    parser_ing.add_argument('files', nargs='+',
                            help="Files (saved by observe --save) to ingest")
    parser_ing.set_defaults(func=run_ingest)

    # "view" subcommand
    parser_viw = subparsers.add_parser('view',
                                       help=run_view.__doc__)
    parser_viw.add_argument('username', nargs='+',
                            help="Username of circle to view")
    parser_viw.set_defaults(func=run_view)

    # "plot" subcommand
    parser_plt = subparsers.add_parser('plot',
                                       help=run_plot.__doc__)
    parser_plt.add_argument('username', help="Username of circle to plot")
    parser_plt.set_defaults(func=run_plot)

    # "audit" subcommand
    parser_adt = subparsers.add_parser('audit',
                                       help=run_audit.__doc__)
    parser_adt.add_argument('--active',
                            dest='query_type', action='store_const', const=1,
                            default=0,
                            help="Only audit active circles")
    parser_adt.add_argument('--betrayed',
                            dest='query_type', action='store_const', const=2,
                            help="Only audit betrayed circles")
    parser_adt.add_argument('--users',
                            dest='query_type', action='store_const', const=-1,
                            help="Only audit betrayed circles")
    parser_adt.add_argument('--staleness', metavar='SECONDS',
                            action='store', type=int,
                            help="Only audit circles with audit this old.")
    parser_adt.add_argument('count', action='store', type=int, default=10,
                            nargs='?',
                            help="Number of audits to perform.")
    parser_adt.set_defaults(func=run_audit)

    # "leaderboard" subcommand
    parser_brd = subparsers.add_parser('leaderboard',
                                       help=run_leaderboard.__doc__)
    parser_brd.add_argument('--update', metavar='POSTID', action='store',
                            help="Update given post ID.")
    parser_brd.add_argument('--full-urls', action='store_true',
                            help="Use full URLs for user links.")
    parser_brd.add_argument('length', action='store', type=int, default=None,
                            nargs='?',
                            help="Return leaderboards of given length.")
    parser_brd.set_defaults(func=run_leaderboard)

    # "export" subcommand
    parser_exp = subparsers.add_parser('export',
                                       help=run_export.__doc__)
    parser_exp.set_defaults(func=run_export)

    # "import" subcommand
    parser_imp = subparsers.add_parser('import',
                                       help=run_import.__doc__)
    parser_imp.set_defaults(func=run_import)

    args = parser.parse_args(args)
    args.func(args)

if __name__ == '__main__':
    main(sys.argv[1:])
