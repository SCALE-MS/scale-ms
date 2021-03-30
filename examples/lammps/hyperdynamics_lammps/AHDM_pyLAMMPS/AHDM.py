import os, sys, shutil, random
import multiprocessing
import numpy as np
from lammps import lammps

def run_lmp(x):
	lmp[x].file(files[x])


# parameters to run HD
time_period = [300000]
Vmax = np.arange(0.00, 0.91, 0.10)
iterations = 200

# locations of the parameters in LAMMPS input file
time_period_line = 19
time_period_col = 1
Vmax_line = 17
Vmax_col = -2
langevin_line = 14
langevin_seed_col = -1

n = len(time_period)*len(Vmax)*iterations # simulations in total

# make LAMMPS input files
lmp = [lammps() for i in range(n)]

for i in range(n):
	shutil.copy('hd.in', 'hd.in.'+str(i))

for i in range(len(Vmax)):
	for j in range(iterations):
		file = []
		with open('hd.in.'+str(i*iterations+j),'r') as f:
			for line in f:
				file.append(list(line.strip('\n').split(' ')))
		file[0][1] = 'log.' + str(i*iterations+j)
		file[time_period_line][time_period_col] = str(time_period[0])
		file[Vmax_line][Vmax_col] = str(Vmax[i])
		file[langevin_line][langevin_seed_col] = str(random.randint(100000, 999999))
		with open('hd.in.'+str(i*iterations+j),'w') as f:
			for k in file:
				for l in k:
					f.write(str(l))
					f.write(' ')
				f.write('\n')
files = []
for i in range(n):
	files.append('hd.in.'+str(i))

# run parallel Python using multi-processors
tasks = range(n)
cores = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=cores)

pool.map(run_lmp, tasks)

# result analysis
count = np.zeros((len(Vmax),iterations//10+1))

for i in range(len(Vmax)):
	for j in range(iterations):
		log = []
		k = j//10+1
		with open('log.'+str(i*iterations+j),'r') as f:
			for line in f:
				log.append(list(line.strip('\n').split(' ')))
		result_line = 0
		with open(r'log.'+str(i*iterations+j),'r') as search:
			for line in search.readlines():
				if 'event' and 'timesteps' in line.strip():
					break
				else:
					result_line += 1
		if log[result_line][-1] != '0':
			for l in range(k, iterations//10+1):
				count[i][l] += 1

probabilities = [0 for i in range(len(Vmax))]
x = range(0, iterations+1, 10)

for i in range(len(probabilities)):
	fit = np.polyfit(x, count[i], 1)
	probabilities[i] = fit[0]

first_step = 0
for i in range(1, len(probabilities)):
	if probabilities[i]-probabilities[i-1]>0.05:
		i += 1
	else:
		first_step = i-1
		break

best_boost_cor = 0
for i in range(first_step+1, len(probabilities)):
	if probabilities[i]-probabilities[i-1]<0.05:
		i += 1
	else:
		best_boost_cor = i-1
		break

with open('result.txt', 'w') as f:
	f.write('count: \n')
	for i in count:
		for j in i:
			f.write(str(j))
			f.write(' ')
		f.write('\n')
	f.write('\n')
	f.write('probabilities: ')
	for i in probabilities:
		f.write(str(i))
		f.write(' ')
	f.write('\n')
	f.write('the first step is at: '+str(Vmax[first_step])+'eV')
	f.write('\n')
	f.write('the best boost is: '+str(Vmax[best_boost_cor])+'eV')