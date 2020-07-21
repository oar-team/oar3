# Example of how to perform actions given usernames stored in a file
if queue != "admin":
    try:
        with open("/tmp/users.txt", "r") as f:
            for line in f:
                if re.search(user, line):
                    queue = "admin"
    except EnvironmentError:
        pass
