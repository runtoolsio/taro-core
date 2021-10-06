from taro.jobs import persistence
from taroapp import cli


def run(args):
    print("Jobs to be removed:")  
    for id in args.id:
        print(str(persistence.num_of_job(id_=id)) + " records found for " + id)
    print("Do you want to continue? [Y/n] ", end="")  
    i = input()
    if i in cli._true_options:
        [persistence.remove_job(id_=id) for id in args.id]