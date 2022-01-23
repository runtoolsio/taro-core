import taro.util
from taro.jobs import persistence


def run(args):
    print("Jobs to be removed:")  
    for id_ in args.id:
        print(str(persistence.num_of_job(id_=id_)) + " records found for " + id_)
    if taro.util.cli_confirmation():
        [persistence.remove_job(id_=id_) for id_ in args.id]
