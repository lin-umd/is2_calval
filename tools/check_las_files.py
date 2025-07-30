FOLDER = '/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/data/las/usa/neon_wref2019'
import laspy
import glob
from dask import compute, delayed
from dask.distributed import Client, LocalCluster
def check_las_file(path):
    try:
        las = laspy.read(path)
        #print(f"{path} loaded successfully.")
        #print(f"Points: {len(las.points)}")
        return True
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return False

if __name__ == "__main__":
    las_files = glob.glob(f"{FOLDER}/*.las")
    if not las_files:
        print("No .las files found.")
    else:
        # change into a dask version 
        # for las_file in las_files:
        #     check_las_file(las_file)
        cluster = LocalCluster(n_workers=10, threads_per_worker=1)
        client = Client(cluster)
        tasks = [delayed(check_las_file)(las_file) for las_file in las_files]
        # determine how many cores i used
        print(f"Number of tasks: {len(tasks)}")
        # compute the results
        results = compute(*tasks)
        # Close the client after computation
        client.close()


