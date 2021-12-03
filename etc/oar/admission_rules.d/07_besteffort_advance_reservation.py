# Verify if besteffort jobs are not an advance reservation
if ("besteffort" in types) and reservation_date:
    raise Exception(
        "[ADMISSION RULE] Error: a job cannot both be of type besteffort and be a reservation."
    )
