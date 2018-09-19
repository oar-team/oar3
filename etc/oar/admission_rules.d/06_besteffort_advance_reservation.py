# Check besteffort is an advance reservation which is not allowed
if ('besteffort' in types) and reservation:
    raise Exception('[ADMISSION RULE] Error: a job cannot both be of type besteffort and be a reservation.')
