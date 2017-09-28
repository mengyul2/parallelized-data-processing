#!/bin/bash
#SBATCH --time=00:30:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
module load Java/1.8.0_71
module load mpj/0.44
javac -cp .:$MPJ_HOME/lib/mpj.jar SearchTwitter.java
mpjrun.sh -np 1 SearchTwitter music  