import argparse
import os
import uuid
from multiprocessing import cpu_count


def parse_input():
    #Create parsers
    parser = argparse.ArgumentParser(description='main_input')
    subparsers = parser.add_subparsers(dest='type', help='sub-command help')
    server_group = subparsers.add_parser('server', help='Arguments specifically for initialising server')
    assoc_group = subparsers.add_parser('assoc', help='Arguments specifically for initialising a gwas')

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

    ########## Arguments for gwas ##########

    assoc_group.add_argument("--host",
                              help="Specify url for master. Defaults to 0.0.0.0",
                              type=str,
                              default="0.0.0.0")

    assoc_group.add_argument("--port",
                              help="Specify port for master. Defaults to 5000",
                              type=int,
                              default=5000)

    assoc_group.add_argument("-bs", "--batch_size",
                        help="Specify how many snps to request from server",
                        type=int,
                        default=cpu_count() * 100)

    assoc_group.add_argument("--model",
                        help="Specify regression model",
                        type=str,
                        required=True)

    assoc_group.add_argument("--bgen",
                        help="Specify path to folder containing bgen files to use in analysis. Files should be names 'chr{chromosome}.bgen'",
                        type=str,
                        required=True)

    assoc_group.add_argument("-gm", "--genetic_model",
                        help="Specify the genetic model to use; additive, dominant or recessive. Default is additive",
                        type=str,
                        default="additive")

    assoc_group.add_argument("-c", "--cores",
                        help="Specify number of cores to use. Default is the number of cores available",
                        type=int,
                        default=cpu_count())

    assoc_group.add_argument("-s", "--sample",
                        help="Specify path to sample file",
                        type=str,
                        default="")

    assoc_group.add_argument("-covar", "--covariates",
                        help="Specify path to covariates file",
                        type=str,
                        default="")

    assoc_group.add_argument("--family",
                            help="Specify regression family. Default is the binomial family",
                            type=str,
                            default="binomial")


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

    if args.type == "assoc":
        host = args.host
        port = args.port
        batch_size = args.bs
        model = args.model
        bgen_path = args.bgen
        gm = args.gm
        cores = args.cores
        sample_path = args.sample
        covar_path = args.covar
        fam = args.family

        import GWAS
        GWAS.run(
            host,
            port,
            batch_size,
            model,
            bgen_path,
            gm,
            cores,
            sample_path,
            covar_path,
            fam
        )
