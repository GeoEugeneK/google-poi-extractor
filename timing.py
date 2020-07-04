
def format_delta(sec: float, include_ms: bool = False):

    assert sec >= 0, 'received negative value as input!'

    result = ""

    d, rem = divmod(sec, 86400)
    if d > 0:
        result += f" {d} d"

    h, rem = divmod(rem, 3600)
    if h > 0:
        result += f" {h} h"

    m, rem = divmod(rem, 60)
    if m > 0:
        result += f" {m} m"

    s, ms = divmod(rem, 1)
    if s:
        result += f" {s} s"

    if include_ms and ms:
        result += f" {int(ms)} ms"

    return result if result else "0 s"
