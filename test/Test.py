import datetime


def check_timedelta():
    now = datetime.date.today()
    print(now)
    now = now + datetime.timedelta(days=-3)
    print(now)


def get_tuple_from_predicate(predicate):
    new_list = predicate.split(" ")
    for i in range(len(new_list)):
        try:
            new_list[i] = datetime.datetime.strptime(new_list[i], '\'%Y-%m-%d\'').date()
        except ValueError:
            pass
    new_tuple = tuple(new_list)
    return new_tuple


p = ['2021-01-01', '2022-12-31', 'hello', 'world']

for i in range(len(p)):
    try:
        p[i] = datetime.datetime.strptime(p[i], '%Y-%m-%d').date()
    except ValueError:
        pass

#print(p)

t = get_tuple_from_predicate("lineitem >= '1995-12-01'")
#print(t)
check_timedelta()
