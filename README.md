# RLQ - Rate Limit Queue

RLQ is a work queue designed to handle a high volume of jobs that must respect complex rate limiting scenarios.

For example a single job could be limited by all of:
- a global overall limit - no more than N jobs should be running concurrently
- a per "user" limit - a specific user cannot run more than N jobs in a time period T
- a per "org" limit - all users in an org cannot run more than N jobs in a time period T
- limits based on API keys that are being used in the job

Under the hood, it uses
- [KeyDB](https://docs.keydb.dev/docs/intro/#what-is-keydb) which is basically fancy fork of redis
  - supports expiration of set members natively
  - supports CRON - to trigger a script natively
  - claims to be faster and better at scaling (multithreaded)
  - will soon support running a script on key expiry, which would simplify a lot of things too
- [ModJS](https://github.com/JohnSully/ModJS) which is a redis/keydb module that lets us write custom scripts in JS instead of Lua


