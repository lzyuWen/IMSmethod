import random
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.visualization.dfg import visualizer as dfg_visualizer



def log_import(file):
    log = dict()


    event_log = xes_importer.apply(file)


    for trace in event_log:
        caseid = trace.attributes['concept:name'] if 'concept:name' in trace.attributes else None
        for event in trace:
            task = event['concept:name']
            if caseid not in log:
                log[caseid] = []
            log[caseid].append(task)
    return log


def find_loop_activity(log):
    loop_activities = set()
    task_num = dict()
    for caseid in log:
        for event in log[caseid]:
            if  event[0] not in task_num:
                task_num[event[0]] = 1
            else:
                task_num[event[0]] += 1
        for key, value in task_num.items():
            if value > 1 and key not in loop_activities:
                loop_activities.add(key)
        task_num.clear()
    return loop_activities




def pre_next(log, loop_activities):
    predecessors = {act: set() for act in loop_activities}
    successors = {act: {} for act in loop_activities}

    for caseid, tasks in log.items():
        for i, task in enumerate(tasks):
            if task in loop_activities:
                # 前驱
                if i > 0:
                    prev_task = tasks[i-1]
                    predecessors[task].add(prev_task)
                # 后继
                if i < len(tasks) - 1:
                    next_task = tasks[i+1]
                    successors[task][next_task] = successors[task].get(next_task, 0) + 1


    predecessors = {act: list(pre) for act, pre in predecessors.items()}

    return predecessors, successors



def filter_loop_successors(successors, loop_activities):
    filtered = {act: {} for act in loop_activities}
    for act, next_acts in successors.items():
        if act in loop_activities:
            for next_act, count in next_acts.items():
                if next_act in loop_activities:
                    filtered[act][next_act] = filtered[act].get(next_act, 0) + count

    return filtered


def shift (filtered_successors):
    dfg = {
        (source, target): weight
        for source, targets in filtered_successors.items()
        for target, weight in targets.items()
    }
    return  dfg

def draw_dfg(dfg):

    parameters = {
        "format": "png",
        "bgcolor": "white",
        "font_size": 12,
        "rankdir": "LR"
    }


    gviz = dfg_visualizer.apply(
        dfg0=dfg,
        parameters=parameters,
        variant=dfg_visualizer.Variants.FREQUENCY
    )


    dfg_visualizer.view(gviz)


def stratified_layering(log, loop_activities):
    layer_1 = dict()
    layer_2 = dict()
    loop_set = set(loop_activities)

    for caseid, activities in log.items():
        activities_list = list(activities)


        loop_activities_in_case = loop_set.intersection(set(activities_list))
        all_present = (loop_activities_in_case == loop_set)

        if all_present:
            layer_1[caseid] = activities
        else:
            layer_2[caseid] = activities

    return layer_1, layer_2


def stratified_sampling_v2(layer_1, layer_2, total_sample_size):

    sampled_log = dict()
    len_layer_1 = len(layer_1)
    len_layer_2 = len(layer_2)

    if len_layer_1 >= total_sample_size:

        sampled_items = random.sample(list(layer_1.items()), total_sample_size)
        sampled_log = dict(sampled_items)
    else:

        sampled_log.update(layer_1)
        remaining = total_sample_size - len_layer_1

        if len_layer_2 > 0:
            sample_2_size = min(remaining, len_layer_2)
            sampled_items_2 = random.sample(list(layer_2.items()), sample_2_size)
            sampled_log.update(dict(sampled_items_2))


    return sampled_log

def determine_sampling_ratio_v2(num_traces):
    LARGE_THRESHOLD = 10000
    MEDIUM_THRESHOLD = 1000

    if num_traces >= LARGE_THRESHOLD:
        return 0.05
    elif num_traces >= MEDIUM_THRESHOLD:
        return 0.10
    else:
        return 0.15

def full_pipeline_v2(file, loop_activities, desired_sample_count=None):
    log = log_import(file)
    num_traces = len(log)
    sample_ratio = determine_sampling_ratio_v2(num_traces)

    if desired_sample_count is None:
        desired_sample_count = int(num_traces * sample_ratio)
    else:
        desired_sample_count = min(desired_sample_count, num_traces)

    layer_1, layer_2 = stratified_layering(log, loop_activities)

    sampled_log = stratified_sampling_v2(layer_1, layer_2, desired_sample_count)


    return sampled_log


def count_direct_and_indirect_follow(sampled_log, group1, group2):

    group1_set = set(group1)
    group2_set = set(group2)
    direct_follow_counts = dict()
    indirect_follow_counts = dict()

    for trace in sampled_log.values():
        i = 0
        while i < len(trace):
            a_from = trace[i]
            if a_from not in group1_set:
                i += 1
                continue


            if i + 1 < len(trace) and trace[i + 1] in group2_set:
                a_to = trace[i + 1]
                direct_follow_counts[(a_from, a_to)] = direct_follow_counts.get((a_from, a_to), 0) + 1
                i += 2
                continue


            found = False
            for j in range(i + 2, len(trace)):
                a_to = trace[j]
                if a_to in group2_set:
                    indirect_follow_counts[(a_from, a_to)] = indirect_follow_counts.get((a_from, a_to), 0) + 1
                    i = j + 1
                    found = True
                    break

            if not found:
                i += 1

    return direct_follow_counts, indirect_follow_counts



def draw_indirect_dfg(indirect_follow_counts):

    parameters = {
        "format": "png",
        "bgcolor": "white",
        "font_size": 12,
        "rankdir": "LR"
    }

    gviz = dfg_visualizer.apply(
        dfg0=indirect_follow_counts,
        parameters=parameters,
        variant=dfg_visualizer.Variants.FREQUENCY
    )
    dfg_visualizer.view(gviz)


