from datetime import datetime

import decimal


def id(rec, name, fieldlen):
    if name in rec and rec[name] != None:
        if len(rec[name]) > 15:
            return rec[name][0:15]
        return rec[name]
    return None


def inte(rec, name, fieldlen):
    if name in rec and rec[name] is not None:
        return rec[name]
    return None


def bl(rec, name, fieldlen):
    if name in rec and rec[name] is not None:
        return rec[name]
    return None


def dt(rec, name, fieldlen):
    if name in rec and rec[name] is not None:
        return py_date(rec[name])
    return None


def ts(rec, name, fieldlen):
    if name in rec and rec[name] is not None:
        return py_timestamp(rec[name])
    return None


def db(rec, name, fieldlen):
    if name in rec and rec[name] is not None:
        d = decimal.Decimal(rec[name])
        s = str(d)
        if 0 < fieldlen < len(s):
            # truncate
            return float(s[0:fieldlen])
        return rec[name]
    return None


def st(rec, name, fieldlen=0):
    if name in rec and rec[name] is not None:
        node = rec[name]
        return scrub(node[0:fieldlen])
    return None


def stsub(rec, name, subname, fieldlen=0):
    if name in rec and rec[name] is not None:
        node = rec[name]
        if subname in node:
            val = node[subname][0:fieldlen]
            return scrub(val)
        return None
    return None


def py_timestamp(t) -> datetime:
    return datetime.strptime(t[0:19], '%Y-%m-%dT%H:%M:%S')


def py_date(d) -> datetime:
    return datetime.strptime(d, '%Y-%m-%d').date()


def scrub(s):
    if '\\t' in s or '\0' in s:
        s = s.replace('\\t', ' ')
        s = s.replace('\0', '')
    return s
