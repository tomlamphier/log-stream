# log-stream
This repo contains a Maven project to build a Spark program named LogStream.  LogStream reads a realtime stream of HTTP log data and looks for a pattern of repeated POST attempts over short intervals.  This pattern is a common type of brute force attack on the Web, with the intent of guessing your logon credentials.  Any malicious IP addresses found by LogStream are saved in a Redis database.  Other applications can then use the database to filter out bad IP addresses.  Yhe Redis db is, in effect, an IP blacklist.

All of this is part of a larger hacker blacklist project, which is written up in [this blog post](http://datasciex.com/?p=161).  
