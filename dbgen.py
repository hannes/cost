import subprocess
import os

from multiprocessing import Pool


sf = 100
splits = 100
dbgen_dir = '/export/scratch2/home/hannes/tpch-kit/dbgen'
out_dir = '/export/scratch2/home/hannes/cost/sf%d' % sf


def generate_split(split):

	params = '-f -C %d -S %d -T L -q -s %s' % (splits, split, sf)
	subprocess.run(['./dbgen'] + params.split(' '), cwd=dbgen_dir)
	split_name = 'orders.tbl.%d' % split
	os.rename(os.path.join(dbgen_dir, split_name), os.path.join(out_dir, split_name))

	if not os.path.isfile(os.path.join(out_dir, split_name)):
		raise ValueError('missing output')

p = Pool(10)

p.map(generate_split, range(1, splits+1))


