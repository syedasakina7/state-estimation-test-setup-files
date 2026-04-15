% se_master.m
% Copyright 2025 Tampere University
% Author: Syeda Sakina Haider


%----------Checking input data-----------------------

input_root = 'input_data';
output_root = 'output_data';

% Get folder list sorted by time (newest first),
folders = dir(input_root);
folders = folders([folders.isdir]);
folder_names = {folders.name};
is_hidden = cellfun(@(f) strncmp(f, '.', 1), folder_names);
folders = folders(~is_hidden);
[~, idx] = sort([folders.datenum], 'descend');
sorted_folders = folders(idx);

if length(sorted_folders) < 1
    error('No input folders found.');
end

%Checks if folder argument was passed from Python
args = argv();
if length(args) < 1
    error('No folder argument passed from Python.');
end

timestamp = args{1};
input_path = fullfile(input_root, timestamp);
output_path = fullfile(output_root, timestamp);


% Check for missing measurements
required_files = {'Voltage.mat', 'Current.mat'};
missing_files = {};
for i = 1:length(required_files)
    filepath = fullfile(input_path, required_files{i});
    if ~exist(filepath, 'file')
        missing_files{end+1} = required_files{i};
    end
end

% Try fallback from previous folder if needed(if measurement missing)
if ~isempty(missing_files) && length(sorted_folders) >= 2
    prev_folder = sorted_folders(2).name;
    prev_path = fullfile(input_root, prev_folder);

    for i = 1:length(missing_files)
        src = fullfile(prev_path, missing_files{i});
        dest = fullfile(input_path, missing_files{i});
        if exist(src, 'file')
            fprintf('Copying fallback %s from previous folder %s\n', miss-ing_files{i}, prev_folder);
            copyfile(src, dest);
        else
            warning('Fallback file %s not found in previous folder.', miss-ing_files{i});
        end
    end
end

% Logging
fprintf('Using input folder: %s\n', input_path);
fprintf('Output folder will be: %s\n', output_path);



%--------------------Load input grid data and measurements------------------------------

load(fullfile(input_path, 'Bus.mat'));      % Bus matrix
load(fullfile(input_path, 'Line.mat'));       % Line matrix
load(fullfile(input_path, 'Voltage.mat'));    % V measurement kV which we need from Loadflow.py, Vk
load(fullfile(input_path, 'Current.mat'));     % current meas A which we need from Loadflow.py, used for Ikm(line currents)
                                               %and also could be used di-rectly for Ik (load currents at buses)
load(fullfile(input_path, 'BaseVoltage.mat'));% vector kV
load(fullfile(input_path, 'Sbase.mat'));      % scalar kVA
load(fullfile(input_path, 'Pkm.mat'));
load(fullfile(input_path, 'Qkm.mat'));
load(fullfile(input_path, 'Ik.mat'));
load(fullfile(input_path, 'Pk.mat'));
load(fullfile(input_path, 'Qk.mat'));


load 20kV_transformer_profiles_v3.mat

if exist('info2025_2027.mat', 'file')
    load('info2025_2027.mat');
else
    error('info2025_2027.mat file is required but not found');
end


time_now = datenum(clock);
hour_id = find(((info2025_2027(:,1) - time_now) < 0),1,'last');
temperature_depencency_id = round(info2025_2027(hour_id,5)/2);


% ------------------Load temperature data coming from mqtt and corrects the P and Q load values------------------------------------

load(fullfile(input_path, 'Temperature.mat')); %this is the real time temper-ature coming from mqtt

profile_P = transformerProfile_P(hour_id,:); % these are calender adjusted values of P
profile_Pstd = transformerProfile_Pstd(hour_id,:);% these are calender ad-justed values of Standard deviation
TemperatureDependency = transformerTemperatureDependen-cies(temperature_depencency_id,:); % temperature dependency

Tave24h = Temperature; %takes the real time temperature measurement from mqtt

temperatureTarget = Tave24h;
temperatureModel = info2025_2027(hour_id,2);
Tdiff = temperatureModel - temperatureTarget;
profile_P_target = profile_P - TemperatureDependency*Tdiff; % real time tem-perature corrected P load values

profile_Q = transformerProfile_Q(hour_id,:);
profile_Qstd = transformerProfile_Qstd(hour_id,:);
% Note: no temperature correction for reactive powers

%-------------- reconstructing the Bus matrix based on corrected load values-----------------------------

load('TransformerMapping1.mat');  % Loads: BusNumber, DSlabel, PowerFactors

% System base in kVA
S_base = 100;

% ---Count how many buses share each DSLabel ---
[unique_ds, ~, ds_idx_group] = unique(DSlabel);
counts = accumarray(ds_idx_group, 1);  % number of buses for each DS label
%especially done for 3 winding transformers secondary and tertiary

% ---Loop over each bus and assign divided load ---
for i = 1:length(BusNumber)
    bus_id = BusNumber(i);
    ds_id  = DSlabel(i);
    pf     = PowerFactors(i);

    if ds_id == 0
        % Cable or no-load: zero everything
        Bus(bus_id, 6:7)  = 0;
        Bus(bus_id,11)    = 0;
        Bus(bus_id,12)  = 0;
        continue;
    end

    % Find which DSLabel group this belongs to
    idx_ds = find(unique_ds == ds_id, 1);
    n_buses_same_ds = counts(idx_ds);

    % Find corresponding transformer index
    idx_tr = find(transformerIDs == ds_id, 1);
    if ~isempty(idx_tr)
        base_kW = S_base * pf; % real power base
        base_kVAr = S_base * sqrt(1 - pf^2); %reactive power base

        P_target = profile_P_target(idx_tr);
        Q_target = profile_Q(idx_tr);
        SD_targetP = profile_Pstd(idx_tr);
        SD_targetQ = profile_Qstd(idx_tr);

        % Convert to per unit (and divide among buses sharing DS, secondary and tertiary sides)
        %powers are divided into 2 for each bus secondary and tertiary of the transformer
        P_pu  = (P_target / base_kW) / n_buses_same_ds;
        Q_pu  = (Q_target / base_kVAr) / n_buses_same_ds;
        SD_pu_P = (SD_targetP / base_kW) / n_buses_same_ds;
        SD_pu_Q = (SD_targetQ / base_kVAr) / n_buses_same_ds;

        % Assign updated powers and SDs to Bus matrix
        Bus(bus_id, 6)  = P_pu;
        Bus(bus_id, 7)  = Q_pu;
        Bus(bus_id,11)  = SD_pu_P;
        Bus(bus_id,12)  = SD_pu_Q;

    else
        % No transformer found
        Bus(bus_id, 6:7)  = 0;
        Bus(bus_id,11)    = 0;
        Bus(bus_id,12)  = 0;
    end
end

% adding generation models
Bus391= 2000/ (0.98*100);
Bus390= 2000/ (0.98*100);
Bus483= 1900/ (0.98*100);
% update the bus matrix column 4 (generation)
Bus(391,4)= Bus391;
Bus(390,4)= Bus390;
Bus(483,4)= Bus483;

% adding shunt element in bus 2 (already in per unit)
Bus(2, 9)= -28.554;

% adding the power losses (conductance G) in load power so, column 6 has Pto-talload= Ploss+Pload(temp adj), Ploss= V^2 * G

load('G.mat');  % G values for each bus in per unit (used G/2 to divide it between 2 connected buses)

V_bus = Bus(:, 2);  % Voltage values for each bus

V_squared = V_bus.^2;

% Compute Ploss = V^2 * G (element-wise multiplication)
Ploadloss = V_squared .* G;  % Element-wise multiplication

% updated load power with power losses  Ptotalload= Pload(temp adjusted load power)+Ploss(conductance G)
Bus(:, 6) = Bus(:, 6) + Ploadloss;

% making 0 values of B non-zero so that there is no inF error
Line(Line(:,5)==0,5)=1e-8;

% -------------------Construct measurement matrices to get perunit values-----------


BaseVoltage = double(BaseVoltage);
Sbase = double(Sbase);
Ib1 = Sbase / (sqrt(3) * BaseVoltage(1)); %base for primary bus 1
Ib = Sbase / (sqrt(3) * BaseVoltage(2)); %base for 20kV buses
Ib = double(Ib);
Ib1 = double(Ib1);

Ikm=[]; % johtovälien virtamittaukset / current flow between nodes
Vk=[];  % solmupisteiden jännitemittaukset / Voltage measurements at nodes
dT = 0;

% ---------------------------Run state estimation engine------------------------


[V, Iline, Iline_middle, Sload, Sline, Sline_middle, Sloss] = ...
    tilaestimaattori(Bus, Line, Pkm, Qkm, Ikm, Vk, Ik, Pk, Qk, dT, 'y');


%--------------- Convert results back to actual units---------------------------

V = V .* BaseVoltage';

Sloss = Sloss .* Sbase;

Iline_actual = Iline .* Ib;


% ------------------------Save output results (actual units)-------

if ~exist(output_path, 'dir')
    mkdir(output_path);
end

save('-mat-binary', fullfile(output_path, 'V.mat'), 'V');
save('-mat-binary', fullfile(output_path, 'Iline_actual.mat'), 'Iline_actual');
save('-mat-binary', fullfile(output_path, 'Sloss.mat'), 'Sloss');

fprintf('Saving mat files...\n');
