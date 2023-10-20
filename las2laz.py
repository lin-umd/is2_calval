import os, multiprocessing, tqdm, argparse, sys, glob
import pandas as pd

LASBIN = '/gpfs/data1/vclgp/decontot/repos/LAStools/bin/'
LAS2LAS = os.path.join(LASBIN, 'las2las')
LASINDEX = os.path.join(LASBIN, 'lasindex')

def getCmdArgs():
    p = argparse.ArgumentParser(description = "Convert point clouds in parallel using a native `las2las` from LAStools.")
    p.add_argument("-i", "--input", dest="input", required=False, type=str, help = "single input file")
    p.add_argument("-l", "--list", dest="list", required=False, type=str, help="file path with input [las] files list OR directory containing las/laz files")
    p.add_argument("-n", "--n_cpu", dest="n_cpu", required=False, type=int,  default=20, help="maximum number of cpu cores to use [20]")
    p.add_argument("-x", "--index", dest="index", required=False, action="store_true", help="create lax index files for outputs")
    p.add_argument("-o", "--output", dest="output", required=False, type=str, help="output file directory - if not declared outputs are saved at the same path as inputs")
    p.add_argument("-e", "--in_format", dest="in_format", required=False, type=str, default='las', help="input file format (if `-l` = a directory)")
    p.add_argument("-f", "--format", dest="format", required=False, type=str, default='laz', help="output file format")
    p.add_argument("-r", "--rm", dest="rm", required=False, action="store_true", help="delete inputs after conversion")
    p.add_argument("-a", "--args", dest="args", required=False, type=str, help="further las2las command line arguments")
    p.add_argument("-s", "--suffix", dest="suffix", required=False, type=str, help="output file name suffix")
    p.add_argument("-g", "--logfile", dest="logfile", required=False, type=str, default="las_log.txt", help="output path for process log file")
    p.add_argument("-d", "--dump", dest="dump", required=False, action='store_true', help="delete input files in bulk if all processes are successful")
    cmdargs = p.parse_args()
    return cmdargs    

def las2las(file, format='laz', root=None, suffix=None, args=None):
    odir = root if root else os.path.dirname(file)    
    cmd = f'{LAS2LAS} -i "{file}" -odir "{odir}" -o{format}'

    out_file = os.path.basename(file).split('.')[0] + f".{format}"
    if suffix:
        cmd += f' -odix "{suffix}"'
        n, fmt = out_file.split('.')        
        out_file = f"{n}{suffix}.{fmt}"

    if args:
        cmd += f' {args}'    
    
    out_file = os.path.join(odir, out_file)

    return cmd, out_file

def proc(obj):
    stdout = os.system(obj.get('cmd'))
    f = obj.get('file')

    if stdout == 0 and os.path.exists(f):
        if obj.get('index'):
            os.system(f'{LASINDEX} -i "{f}"')
        if obj.get('rm'):
            f_in = obj.get('input')
            os.system(f'rm "{f_in}"')
    
    return f, stdout

def proc_parallel(objs, cpus):    
    dflog = pd.DataFrame(columns=['file', 'status'])
    n_cores = cpus if cpus <= multiprocessing.cpu_count() else multiprocessing.cpu_count()/2
    
    pool = multiprocessing.Pool(n_cores)
    for f, stt in tqdm.tqdm(pool.imap_unordered(proc, objs), total=len(objs)): 
        dflog = dflog.append({'file':f, 'status': stt}, ignore_index=True)
        
    pool.close()
    return dflog

def main(args):
    if not os.path.isdir(args.output):
        os.makedirs(args.output, exist_ok=True)
    if args.list:
        if os.path.isdir(args.list):
            files = glob.glob(args.list + '/*.' + args.in_format.lower())
            files += glob.glob(args.list + '/*.' + args.in_format.upper())
        else:
            files = pd.read_csv(args.list, delim_whitespace=True, header=None).iloc[:,0].to_list()
        
        if len(files) == 0:
            return
        
        cmds = [las2las(f, args.format, args.output, args.suffix, args.args) for f in files]
        objs = [{'input':files[i], 'file':j[1], 'cmd': j[0], 'index': args.index, 'rm':args.rm and not args.dump} for i, j in enumerate(cmds)]
        log_file = proc_parallel(objs, args.n_cpu)
        log_file.to_csv(args.logfile, index=False)
        
        if args.dump:
            status = log_file.status.sum()
            ofiles = glob.glob(args.output + '/*.' + args.format)
            if status == 0 and len(files) == len(ofiles):
                if os.path.isdir(args.list):
                    os.system(f"rm -rf {args.list}")
                else:
                    [os.unlink(i) for i in files]
    else:
        cmd, f = las2las(args.input, args.format, args.output, args.suffix, args.args)
        obj = {'input':args.input, 'file':f, 'cmd': cmd, 'index': args.index, 'rm': args.rm}
        f, stt = proc(obj)
        if stt != 0:
            print(f"ERROR processing {args.input}")
        else:
            print(f"DONE processing {f}")

if __name__ == '__main__':
    args = getCmdArgs()
    
    if not args.list and not args.input:
        sys.exit("--list or --input must be provided")

    main(args)