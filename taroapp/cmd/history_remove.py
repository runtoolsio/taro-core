import taro.util
from taro.jobs import persistence


def run(args):
    print("Jobs to be removed:")  
    for id_ in args.id:
        print(str(persistence.num_of_job(id_=id_)) + " records found for " + id_)
    print("Do you want to continue? [Y/n] ", end="")  
    i = input()
    if i.lower() in taro.util.TRUE_OPTIONS:
        [persistence.remove_job(id_=id_) for id_ in args.id]
