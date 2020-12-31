import argparse
import os
import uuid

def parse_input():
    #Create parsers
    parser = argparse.ArgumentParser(description='main_input')
    subparsers = parser.add_subparsers(dest='type', help='sub-command help')
    server_group = subparsers.add_parser('server', help='Arguments specifically for initialising server')

    ########## Arguments for server ##########
    server_group.add_argument("--host",
                              help="Specify url for master. Defaults to 0.0.0.0",
                              type=str,
                              default="0.0.0.0")

    server_group.add_argument("--port",
                              help="Specify port for master. Defaults to 5000",
                              type=int,
                              default=5000)

    server_group.add_argument("--data",
                              help="Specify path to database containing data to use in analysis",
                              type=str,
                              required=True)

    server_group.add_argument("-o", "--out",
                              help="Specify where to output results",
                              type=str,
                              default=os.getcwd() + "/" + str(uuid.uuid4()) + ".sqlite3")

    server_group.add_argument("-chr", "--chromosomes",
                              help="Specify chromosomes to analyse",
                              type=int,
                              nargs='+',
                              default=[i for i in range(1, 23)])


    return parser.parse_args()

if __name__ == "__main__":

    args = parse_input()

    if args.type == "server":
        from ServiceManager import ServiceManager
        import Webservice

        data = args.data
        out = args.out
        host = args.host
        port = args.port
        chromosomes = args.chromosomes

        service_manager = ServiceManager(
            data,
            out,
            chromosomes=chromosomes,
            verbose=True
        )

        Webservice.init_service(host, port, service_manager, verbose=True)
