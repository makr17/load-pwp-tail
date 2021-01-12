# load-pwp-tail
Small utility to read meter consumption data from STDIN and log to
postgresql db.  PWP is my local water and power utility.

I get the data from an SDR attached to a raspberry pi running
[rtlamr](https://github.com/bemasher/rtlamr),
outputting CSV and piping into `load-pwp-tail`.
