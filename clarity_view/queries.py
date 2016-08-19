from collections import defaultdict, OrderedDict
import genologics_sql.tables as t

SAMPLE_SUB_STEPS = set(['Bartender label generation EG 2.0'])
SAMPLE_QC_STEPS = set(['Eval Project Quant'])
LIBRARY_PREP_STEPS = set(['Read and Eval SSQC'])
LIBRARY_QC_STEPS = set(['Eval qPCR Quant'])
SEQUENCING_STEPS = set(['AUTOMATED - Sequence'])
OTHER_STEPS = set([])
sample_statuses = OrderedDict((
    ('Other', OTHER_STEPS),
    ('Sample Submission', SAMPLE_SUB_STEPS),
    ('Sample QC', SAMPLE_QC_STEPS),
    ('Library Preparation', LIBRARY_PREP_STEPS),
    ('Library QC', LIBRARY_QC_STEPS),
    ('Sequencing', SEQUENCING_STEPS)
))




def get_samples_and_processes(session, project_name=None, list_process=None, workstatus=None, only_open_project=True):
    """This method runs a query that return the sample name and the processeses they went through"""
    q = session.query(t.Project.name, t.Sample.name, t.ProcessType.displayname, t.Process.workstatus)\
           .distinct(t.Sample.name, t.Process.processid)\
           .join(t.Sample.project)\
           .join(t.Sample.artifacts)\
           .join(t.Artifact.processiotrackers)\
           .join(t.ProcessIOTracker.process)\
           .join(t.Process.type)
    if list_process:
        q = q.filter(t.ProcessType.displayname.in_(list_process))
    if project_name:
        q = q.filter(t.Project.name == project_name)
    if workstatus:
        q = q.filter(t.Process.workstatus == workstatus)
    if only_open_project:
        q = q.filter(t.Project.closedate == None)
    return q.all()



class Sample():
    def __init__(self):
        self.processes = set()

    @property
    def status(self):
        for status in reversed(sample_statuses):
            if sample_statuses.get(status).issubset(self.processes) :
                return status
        return 'Other'


class Project():
    def __init__(self):
        self.samples = defaultdict(Sample)

    def samples_per_status(self):
        sample_per_status = defaultdict(list)
        for sample_name in self.samples:
            sample_per_status[self.samples[sample_name].status].append(sample_name)
        return sample_per_status

def sample_status_per_project(session, project_name=None):
    all_projects = defaultdict(Project)
    for result in get_samples_and_processes(session, project_name, workstatus='COMPLETE'):
        project_name, sample_name, process_name, process_status = result
        all_projects[project_name].samples[sample_name].processes.add(process_name)

    columns = ['Project'] + list(sample_statuses)
    rows = []
    for project in all_projects:
        row = [project]
        [s.status for s in all_projects[project].samples.values()]
        samples_per_status = all_projects[project].samples_per_status()
        for status in sample_statuses:
            row.append(str(len(samples_per_status.get(status, []))))
        rows.append(row)
    return {'title':'Status', 'cols':columns, 'rows':rows}





def all_processes_per_project(session, project_name=None):
    all_projects = defaultdict(dict)
    all_processes = set()
    for result in get_samples_and_processes(session, project_name, workstatus='COMPLETE'):
        project_name, sample_name, process_name, process_status = result
        all_processes.add(process_name)
        if not process_name in all_projects[project_name]:
            all_projects[project_name][process_name] = set()
        all_projects[project_name][process_name].add(sample_name)

    all_processes = sorted(list(all_processes))
    columns = ['Project'] + all_processes
    rows = []
    for project in all_projects:
        row = [project]
        for process_name in all_processes:
            row.append(str(len(all_projects[project].get(process_name, []))))
        rows.append(row)
    return {'title':'Processes', 'cols':columns, 'rows':rows}



