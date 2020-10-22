import sys
file_ = sys.argv[1]
f = open(file_, "r")
for i, line in enumerate(f,1):
        if i > 124055301: #range(246934820):
            print(line)
