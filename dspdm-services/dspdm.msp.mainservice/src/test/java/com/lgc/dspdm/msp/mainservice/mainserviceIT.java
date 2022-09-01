
package com.lgc.dspdm.msp.mainservice;

import com.lgc.dist.core.msp.test.MTestRule;
import com.lgc.dspdm.msp.mainservice.client.mainservice;
import org.junit.ClassRule;


public class mainserviceIT {
	@ClassRule
	public static MTestRule<mainservice> rule = new MTestRule<mainservice>("mainservice", mainservice.class);
	
}
