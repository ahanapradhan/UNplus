import datetime

def check_attr_list():
    global_filter_aoa = [[1,2,3,4,5], [6,7,8,9,10], [11,12,13,14,15], [16,17,18,19,20]]
    attr_list = []
    for pred in global_filter_aoa:
        attr_list.append((pred[0], pred[1]))
        attr_list.append((pred[3], pred[4]))
    attr_list = list(set(attr_list))
    return attr_list

def check_attr_list1():
    global_filter_aoa = [[1,2,3,4,5], [6,7,8,9,10], [11,12,13,14,15], [16,17,18,19,20]]
    attr_list = []
    for pred in global_filter_aoa:
        attr_list.extend([(pred[0], pred[1]), (pred[3], pred[4])])
    attr_list = list(set(attr_list))
    return attr_list

def check_attr_list2():
    global_filter_aoa = [[1,2,3,4,5], [6,7,8,9,10], [11,12,13,14,15], [16,17,18,19,20]]
    attr_list = {(pred[0], pred[1]) for pred in global_filter_aoa} | {(pred[3], pred[4]) for pred in
                                                                                     global_filter_aoa}
    return attr_list


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

def mid_cal_for_date():
    left = datetime.date(1994, 1, 1)
    right = datetime.date(2000, 1, 1)
    if abs(right - left) > 0:
        mid = (left + right + 1) // 2
    print(mid)

#print(p)

def call_by_ref(la):
    la.append('a')
    print(la)

t = get_tuple_from_predicate("lineitem >= '1995-12-01'")
#print(t)
#check_timedelta()
#mid_cal_for_date()
lb = ['b', 'c']
call_by_ref(lb)
print(lb)
call_by_ref(lb)
print(lb)
