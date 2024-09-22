if types:
    r1 = "^container(?:=\\w+)?$"
    r2 = "^deploy(?:=standby)?$"
    r3 = "^desktop_computing$"
    r4 = "^besteffort$"
    r5 = "^cosystem(?:=standby)?$"
    r6 = "^idempotent$"
    r7 = "^placeholder=\\w+$"
    r8 = "^allowed=\\w+$"
    r9 = "^inner=\\w+$"
    r10 = "^timesharing=(?:(?:\\*|user),(?:\\*|name)|(?:\\*|name),(?:\\*|user))$"
    r11 = "^noop(?:=standby)?$"
    r12 = "^envelope$"
    r13 = "^leaflet=\\w+$"
    r14 = "^supersed=\\w+$"

    all_re = re.compile(
        "(%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s)"
        % (r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14)
    )

    for t in types:
        if not re.match(all_re, t):
            raise Exception("[ADMISSION RULE] Error: unknown job type: " + t)
