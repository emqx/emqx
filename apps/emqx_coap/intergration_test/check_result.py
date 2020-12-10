import sys


def have_string(filename, text):
    data = open(filename, "rb").read()
    if data.find(text) > 0:
        return True
    else:
        return False
    

def mark(case_number, result, description):
    if result:
        f = open(case_number+"_PASS.txt", "wb")
        f.close()
        print("\n\n"+case_number+"     PASS\n\n")
    else:
        f = open(case_number+"_FAIL.txt", "wb")
        f.write(description)
        f.close()
        print("\n\n"+case_number+"     FAIL\n\n")
    
def parse_condition(condition):
    if condition.find("==") > 0:
        r = condition.split("==")
        return r[0], r[1], True
    elif condition.find("!=") > 0:
        r = condition.split("!=")
        return r[0], r[1], False
    else:
        print("\ncondition syntax error\n\n\n")
        sys.exit("condition syntax error")
    
    
def main():
    case_number = sys.argv[1]
    description = ""
    conclustion = True
    for condition in sys.argv[2:]:
        filename, text, result = parse_condition(condition)
        if have_string(filename, text) == result:
            pass
        else:
            conclustion = False
            description = description + "\n" + condition + " failed\n"
    
    mark(case_number, conclustion, description)
    
    
if __name__ == "__main__":
    main()
    
