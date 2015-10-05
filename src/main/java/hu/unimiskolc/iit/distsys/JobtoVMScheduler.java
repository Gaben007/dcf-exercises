package hu.unimiskolc.iit.distsys;

import java.util.Collection;
import java.util.List;

import hu.mta.sztaki.lpds.cloud.simulator.DeferredEvent;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.unimiskolc.iit.distsys.interfaces.BasicJobScheduler;

public class JobtoVMScheduler {
	public JobtoVMScheduler(Collection<VirtualMachine> vmset, List<Job> jobs)
			throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		final BasicJobScheduler mySched = TestCreatorFactory.createARoundRobinScheduler();
		mySched.setupVMset(vmset);
		for (final Job j : jobs) {
			new DeferredEvent(j.getStartTimeInstance()) {
				@Override
				protected void eventAction() {
					mySched.handleJobRequestArrival(j);
				}
			};
		}
	}
}
