import os, sys, shutil, random
import multiprocessing
import numpy as np
from lammps import lammps

def run_lmp(x):
	lmp[x].file(files[x])


# parameters
time_period = np.arange(500000, 700001, 200000)
Vmax = np.arange(0.25, 0.75, 0.05)
iterations = 200

stair_threshold = 0.04
prediction = []

# locations of the parameters in LAMMPS input file
time_period_line = 19
time_period_col = 1
Vmax_line = 17
Vmax_col = -2
langevin_line = 14
langevin_seed_col = -1

total_iterations = len(time_period)*len(Vmax)*iterations

# make LAMMPS input files
lmp = [lammps() for i in range(total_iterations)]

in_file = []
with open('hd.in', 'r') as f:
	for line in f:
		in_file.append(list(line.strip('\n').split(' ')))

for i in range(len(time_period)):
	in_file[time_period_line][time_period_col] = str(time_period[i])
	for j in range(len(Vmax)):
		in_file[Vmax_line][Vmax_col] = str(Vmax[j])
		for k in range(iterations):
			in_file[0][1] = 'log.' + str(i*len(Vmax)*iterations+j*iterations+k)
			in_file[langevin_line][langevin_seed_col] = str(random.randint(100000, 999999))
			with open('hd.in.'+str(i*len(Vmax)*iterations+j*iterations+k), 'w') as f:
				for l in in_file:
					for m in l:
						f.write(str(m)+' ')
					f.write('\n')

files = []
for i in range(total_iterations):
	files.append('hd.in.'+str(i))

# run parallel Python using multi-processors
tasks = range(total_iterations)
cores = multiprocessing.cpu_count()
pool = multiprocessing.Pool(processes=cores)

pool.map(run_lmp, tasks)

# result analysis
for i in range(len(time_period)):
	count = np.zeros((len(Vmax), iterations//10+1))
	for j in range(len(Vmax)):
		for k in range(iterations):
			l = k//10+1
			result_line = 0
			with open('log.' + str(i*len(Vmax)*iterations+j*iterations+k), 'r') as search:
				for line in search.readlines():
					if 'event' and 'timesteps' in line.strip():
						break
					else:
						result_line += 1
			log = []
			with open('log.' + str(i*len(Vmax)*iterations+j*iterations+k), 'r') as f:
				for line in f:
					log.append(list(line.strip('\n').split(' ')))
			if log[result_line][-1] != '0':
				for a in range(l, iterations//10+1):
					count[j][a] += 1
	probabilities = [0 for i in range(len(Vmax))]
	x = range(0, iterations+1, 10)
	for k in range(len(probabilities)):
		fit = np.polyfit(x, count[k], 1)
		probabilities[k] = fit[0]
	if probabilities[0]<0.04 and probabilities[1]<0.04 and probabilities[-1]>0.4 and probabilities[-1]<0.95 and probabilities[-2]<0.95:
		ground_last = 0
		for i in range(1, len(probabilities)):
			if probabilities[i]-probabilities[i-1]<stair_threshold:
				i += 1
			else:
				ground_last = i-1
				break
		first_step = 0
		for i in range(ground_last+1, len(probabilities)):
			if probabilities[i]-probabilities[i-1]>stair_threshold:
				i += 1
			else:
				first_step = i-1
				break
		best_boost_cor = 0
		for i in range(first_step+1, len(probabilities)):
			if probabilities[i]-probabilities[i-1]<stair_threshold:
				i += 1
			else:
				best_boost_cor = i-1
				break
		with open('prediction_t=' + str(time_period[i]) + '.txt', 'w') as f:
			f.write('GOOD RESULT! MAKE A PREDICTION! \n Vmax: ')
			for i in Vmax:
				f.write(str(i) + ' ')
			f.write('\n count: \n')
			for i in count:
				for j in i:
					f.write(str(j) + ' ')
				f.write('\n')
			f.write('probabilities: \n')
			for i in probabilities:
				f.write(str(i) + ' ')
			f.write('\n the first step is at: ' + str(Vmax(first_step) + 'eV \n the best boost is : ' + str(Vmax(best_boost_cor) + 'eV')))
		prediction.append(Vmax[best_boost_cor])
	else:
		with open('prediction_t=' + str(time_period[i]) + '.txt', 'w') as f:
			f.write('BAD RESULT! NO PREDICTION! \n Vmax: \n')
			for i in Vmax:
				f.write(str(i) + ' ')
			f.write('\n count: \n')
			for i in count:
				for j in i:
					f.write(str(j) + ' ')
				f.write('\n')
			f.write('probabilities: \n')
			for i in probabilities:
				f.write(str(i) + ' ')

best_boost = np.argmax(np.bincount(prediction))

with open('best_boost_prediction.txt', 'w') as f:
	f.write('the predictions are: \n')
	for i in prediction:
		f.write(str(i)+' ')
	f.write('\n the best boost is: \n' + str(best_boost))