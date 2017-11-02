#!/usr/bin/env octave -qf

%% usage: octave runmylake.m init par input starty endy outdir

clear all;
path(path, 'v12-kojioctave')
warning('off', 'all') 

arg_list = argv();

## initfile = 'init';
## parfile = 'par'; 
## inputfile = 'input';
## m_start = [1969, 1, 1]; 
## m_stop = [1980, 12, 31]; 
## outdir = '.';


initfile = arg_list{1};
parfile = arg_list{2}; 
inputfile = arg_list{3};
m_start2 = [str2num(arg_list{4}) + 2, 1, 1]; 
m_start = datevec(datenum(m_start2) - 365 - 365)(1:3)
m_stop = [str2num(arg_list{5}), 12, 31]; 
outdir = arg_list{6};

global ies80 Eevapor;
test_time = 0;
Eevapor = 0;

dt = 1.0;

% tic

[In_Z, In_Az, tt, In_Tz, In_Cz, In_Sz, In_TPz, ...
 In_DOPz, In_Chlz, In_DOCz, In_TPz_sed, In_Chlz_sed, ...
 In_FIM, Ice0, Wt, Inflw, ...
 Phys_par, Phys_par_range, Phys_par_names, ...
 Bio_par, Bio_par_range, Bio_par_names] ...
    = modelinputs_v12_1b(m_start, m_stop, initfile, 'lake', ...
                         inputfile, 'timeseries',  parfile, 'lake', ...
                         dt);

% defaults a la Minnesota
if(isnan(Phys_par(2)))
  Phys_par(2) = 0.00706*(In_Az(1)/1e6)^0.56; % default diffusion coeff. parameterisation
end

if(isnan(Phys_par(3)))
  Phys_par(3) = 8.98e-4;		%default value for diffusion coeff. in ice-covered water   
end

if(isnan(Phys_par(4)))
  Phys_par(4) = 7e-5;			% default minimum allowed stability frequency, N2 > N0 <=> Kz < Kmax (1/s2)    		
end

if(isnan(Phys_par(5)))
  Phys_par(5) =  1-exp(-0.3*In_Az(1)/1e6);			% default wind sheltering parameterisation		
end

[zz,Az, Vz, tt, Qst, Kzt, Tzt, Czt, Szt, Pzt, Chlzt, PPzt, DOPzt, DOCzt, ...
 lambdazt, His, DoF, DoM, MixStat, Wt] ...
    = solvemodel_v12_1k(m_start, m_stop, initfile, 'lake', ...
                        inputfile, 'timeseries',  parfile, 'lake', ...
                        In_Z, In_Az, tt, In_Tz, In_Cz, In_Sz, In_TPz, ...
                        In_DOPz, In_Chlz, In_DOCz, In_TPz_sed, In_Chlz_sed, ...
                        In_FIM, Ice0, Wt, Inflw, ...
                        Phys_par, Phys_par_range, Phys_par_names, ...
                        Bio_par, Bio_par_range, Bio_par_names);    
% run_time=toc
f1 = fopen(strcat(outdir, '/Tzt.csv.gz'), 'wbz');
dlmwrite(f1, Tzt(:, 731:end)', 'delimiter', ',', 'precision', '%.3f');
fclose(f1);
f2 = fopen(strcat(outdir, '/His.csv.gz'), 'wbz');
dlmwrite(f2, His(:, 731:end)', 'delimiter', ',', 'precision', '%.3f');
fclose(f2);
f3 = fopen(strcat(outdir, '/Kzt.csv.gz'), 'wbz');
dlmwrite(f3, Kzt(:, 731:end)', 'delimiter', ',', 'precision', '%.3f');
fclose(f3);
f4 = fopen(strcat(outdir, '/Qst.csv.gz'), 'wbz');
dlmwrite(f4, Qst(:, 731:end)', 'delimiter', ',', 'precision', '%.3f');
fclose(f4);
% dlmwrite(strcat(outdir, 'lambdazt.csv'), lambdazt(:, 731:end)', 
%          'delimiter', ',', 'precision', '%.3f')

          
