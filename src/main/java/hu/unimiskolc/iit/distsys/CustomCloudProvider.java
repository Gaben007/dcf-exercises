package hu.unimiskolc.iit.distsys;

import java.util.List;

import org.apache.commons.lang3.RandomUtils;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ResourceConstraints;
import hu.unimiskolc.iit.distsys.forwarders.IaaSForwarder;
import hu.unimiskolc.iit.distsys.interfaces.CloudProvider;

public class CustomCloudProvider implements CloudProvider, VMManager.CapacityChangeEvent<PhysicalMachine>, IaaSForwarder.VMListener {
	private IaaSService myProvidedService;
	private CostAnalyserandPricer costAnalyser;
	private static double basicPrice = 0.00002;
	private static double requiredprofitRate = 1.0;
	
	public static double maxProcessor = 0;
	public static double maxPower = 0;
	public static double maxPowerPerProc = 0;
	public static double allProcessor = 0;
	public static double allRequest = 0;
	public static double requestedVmsCount = 0;
	public static double vmRequestCount = 0;
	public static double sumRequestResults = 0;
	public static double sumEffectiveness = 0;
	public static double sumProc = 0;
	public static double newPmCount = 0;

	@Override
	public double getPerTickQuote(ResourceConstraints rc) {
		maxProcessor = Math.max(rc.getRequiredCPUs(), maxProcessor);
		maxPower = Math.max(rc.getTotalProcessingPower(), maxPower);
		maxPowerPerProc = Math.max(rc.getRequiredProcessingPower(), maxPowerPerProc);
		sumProc += rc.getRequiredCPUs();
		allProcessor += rc.getRequiredCPUs();
		allRequest++;
		//return rc.getTotalProcessingPower() * 0.00000000001;
		//return rc.getTotalProcessingPower() * 0.00000000002;
		//return rc.getRequiredCPUs() * 0.00005;
		
		//double effectiveness = myProvidedService.getRunningCapacities().getTotalProcessingPower() / myProvidedService.getCapacities().getTotalProcessingPower();
		double effectiveness = 1 - (getFreeCapacities() / getTotalCapacities());
		
		double effectivenessBasedDiscount = getEffectivenessBasedDiscount(effectiveness);
		double procCountBasedDiscount = getProcCountBasedDiscount(rc.getRequiredCPUs());
		double successSeelcetionRate = vmRequestCount / allRequest;
		if (successSeelcetionRate < 0.8)
			successSeelcetionRate = 0.8;
		else if (successSeelcetionRate > 1.0)
			successSeelcetionRate = 1.0;
		
		setBasicPriceByCostAnylser();
		
		double result = rc.getRequiredCPUs() * basicPrice * effectivenessBasedDiscount * procCountBasedDiscount * requiredprofitRate;// * successSeelcetionRate;
		sumRequestResults += result / rc.getRequiredCPUs();
		sumEffectiveness += effectiveness;
		return result;
	}
	
	private void setBasicPriceByCostAnylser() {
		if (costAnalyser == null || allRequest < 1000)
			return;
		
		basicPrice = costAnalyser.getTotalCosts() / allProcessor;
	}

	private double getTotalCapacities(){
		double result = 0;
		for (PhysicalMachine pm : myProvidedService.machines) {
			result += pm.getCapacities().getRequiredCPUs();
		}
		return result;
	}
	private double getFreeCapacities(){
		double result = 0;
		for (PhysicalMachine pm : myProvidedService.machines) {
			result += pm.availableCapacities.getRequiredCPUs();
		}
		return result;
	}
	
	private double getEffectivenessBasedDiscount(double effectiveness) {
		
		if (effectiveness < 0.001)
			return 0.5;
		
		if (effectiveness < 0.10)
			return 0.9;
		
		if (effectiveness < 0.30)
			return 0.9;
		
		if (effectiveness < 0.50)
			return 1.0;
		
		if (effectiveness < 0.80)
			return 0.9;
		
		if (effectiveness > 0.90)
			return 1.1;
		
		if (effectiveness > 0.80)
			return 1.05;
		
		return 1.0;
	}
	
	private double getProcCountBasedDiscount(double procCount) {
		if (procCount > 30)
			return 0.6;
		
		if (procCount > 10)
			return 0.8;
		
		if (procCount > 5)
			return 0.85;
		
		if (procCount > 3)
			return 0.95;
		
		return 1.0;
	}

	@Override
	public void setIaaSService(IaaSService iaas) {
		myProvidedService = iaas;
		myProvidedService.subscribeToCapacityChanges(this);
		((IaaSForwarder) myProvidedService).setQuoteProvider(this);
		//((IaaSForwarder) myProvidedService).setVMListener(this);
	}

	@Override
	public void capacityChanged(ResourceConstraints newCapacity, List<PhysicalMachine> affectedCapacity) {
		final boolean newRegistration = myProvidedService.isRegisteredHost(affectedCapacity.get(0));
		if (!newRegistration) {
			try {
				for (PhysicalMachine pm : affectedCapacity) {
					// For every lost PM we buy a new one.
					myProvidedService.registerHost(ExercisesBase.getNewPhysicalMachine(RandomUtils.nextDouble(2, 5)));
					newPmCount++;
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void newVMadded(VirtualMachine[] vms) {
		requestedVmsCount += vms.length;
		vmRequestCount++;
	}

	@Override
	public void setCostAnalyser(CostAnalyserandPricer analyser) {
		costAnalyser = analyser;
	}
}
