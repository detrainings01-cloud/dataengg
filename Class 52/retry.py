# retry.py
#
# WHAT THIS DOES:
#   A simple decorator that auto-retries any function when it fails.
#   Uses exponential backoff — waits longer between each retry.
#
#   Attempt 1 fails → wait 2s → retry
#   Attempt 2 fails → wait 4s → retry
#   Attempt 3 fails → wait 8s → give up and raise error
#
# INSTALL:
#   No extra libraries needed — uses Python built-ins only

import time
import random
import functools


def retry(max_attempts: int = 3, delay: float = 2.0, backoff: float = 2.0):
    """
    Decorator that retries a function on failure.

    Args:
        max_attempts : Total number of tries (default 3)
        delay        : Seconds to wait before first retry (default 2s)
        backoff      : Multiply delay by this after each failure (default 2x)

    Example — what happens with defaults:
        Attempt 1 → fail → wait 2s
        Attempt 2 → fail → wait 4s
        Attempt 3 → fail → RAISE exception

    Usage:
        @retry(max_attempts=3, delay=2.0)
        def extract(): ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            wait    = delay
            last_error = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)

                except Exception as e:
                    last_error = e

                    if attempt == max_attempts:
                        # All attempts used up — give up
                        print(f"[RETRY] ❌ {func.__name__} failed after {max_attempts} attempts.")
                        print(f"[RETRY]    Final error: {e}")
                        raise

                    # Add small random jitter (±20%) so retries don't all hit at same time
                    jitter      = random.uniform(0.8, 1.2)
                    actual_wait = round(wait * jitter, 1)

                    print(f"[RETRY] Attempt {attempt}/{max_attempts} failed: {e}")
                    print(f"[RETRY] Waiting {actual_wait}s before retry...")
                    time.sleep(actual_wait)

                    wait *= backoff   # Double the wait for next attempt

        return wrapper
    return decorator


# ════════════════════════════════════════════════════════
# HOW TO USE IN pipeline.py
# ════════════════════════════════════════════════════════
#
#   from retry import retry
#
#   @retry(max_attempts=3, delay=2.0)
#   def extract():
#       response = requests.get(API_URL, timeout=10)
#       response.raise_for_status()
#       return response.json()
#
#   @retry(max_attempts=3, delay=1.0)
#   def load(df, bucket, key):
#       s3.put_object(...)
