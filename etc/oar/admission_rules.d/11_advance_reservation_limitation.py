# Limit the number of reservations that a user can do.
# (overrided on user basis using the file: ~oar/unlimited_reservation.users)

if reservation_date:
    unlimited = False
    try:
        with open(os.environ["HOME"] + "/unlimited_reservation.users", "r") as f:
            for line in f:
                if re.search(user, line):
                    unlimited = True
    except EnvironmentError:
        pass

    if unlimited:
        print(
            "[ADMISSION RULE] {} is granted the privilege to do unlimited reservations".format(
                user
            )
        )
    else:
        max_nb_resa = 2
        nb_resa = len(
            session.query(Job.id)
            .filter(Job.user == user)
            .filter(or_(Job.reservation == "toSchedule", Job.reservation == "Schedule"))
            .filter(or_(Job.state == "Waiting", Job.state == "Hold"))
            .all()
        )
        if nb_resa >= max_nb_resa:
            raise Exception(
                "[ADMISSION RULE] Error : you cannot have more than $max_nb_resa waiting reservations."
            )
