with open('idx.txt', 'r') as f:
    cur = None
    cnt = 0
    res = 0
    while True:
        line = f.readline()
        if not line:
            break;
        if line == cur:
            cnt += 1
        else:
            cnt = 1
            cur = line
        if cnt == 8:
            res += 1
            cnt = 0
            cur = None
    print(res)