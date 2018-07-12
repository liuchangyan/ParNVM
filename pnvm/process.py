#!/usr/bin/env python3

with open("output.log") as f:
    contents = f.read()
    success = contents.count("true")
    fail = contents.count("false")

    print("Sucess: {}".format(success))
    print("Abort : {}".format(fail))


