import argparse
from tb_lst import commodity_tb_lst, country_tb_lst

def get_args():

    parser = argparse.ArgumentParser(description="Run the table")

    parser.add_argument(
        "--run_year",
        required=True,
        help='Finanacial year in YYYY'
    )

    parser.add_argument(
        "--category",
        required=True,
        choices=["commodity", "country"],
        help="which category to be ran, it shoudl be from the choices provided above"
    )

    args = parser.parse_args()

    return {
        'run_year': args.run_year,
        'category':args.category
    }
def get_tb_lst(category):
    tb_layer = {
        "commodity":commodity_tb_lst,
        "country": country_tb_lst
    }

    return tb_layer[category]


def main():
    args = get_args()
    run_year = args['run_year']
    category = args['category']

    tb_lst = get_tb_lst(category)

    for tb_name in tb_lst.keys():
        RunnerClass = tb_lst[tb_name]
        runner = RunnerClass(run_year = run_year)
        
        with runner:
            runner.run()


if __name__ == "__main__":
    main()