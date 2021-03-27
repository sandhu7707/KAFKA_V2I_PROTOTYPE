import java.util.UUID;

public class CarProps {

    int position;
    int speed;
    String nearestRSUAddress;
    public Boolean RSUAddressGotSwitched = false;                           //TODO: find a way to simulate the switch .. in a way that RSUs can refuse messages or appear disconnected.. add acknowledgements and do an efficient handover to simulate real environment.
    public final int tickTime = 2;
    public final String carId = UUID.randomUUID().toString();

    CarProps(){
        position = 0;
        speed = (int) ((Math.random() * (30)) + 5);  // speed between 5 to 35 m/s
        automatedRSUAllocator();
    }

    void automatedRSUAllocator(){

        if(0 <= position && position < 500){
            if(nearestRSUAddress == null || !nearestRSUAddress.equals(firstRSUAddress)){
                RSUAddressGotSwitched = true;
                System.out.println(carId + " can now only produce on RSU @ " + firstRSUAddress);
            }
            nearestRSUAddress = firstRSUAddress;
        }

        else if(500 <= position && position < 1000){
            if(!nearestRSUAddress.equals(secondRSUAddress)){
                RSUAddressGotSwitched = true;
                System.out.println(carId + " can now only produce on RSU @ " + secondRSUAddress );
            }
            nearestRSUAddress = secondRSUAddress;
        }

        else if(1000 <= position && position < 1500){
            if(!nearestRSUAddress.equals(thirdRSUAddress)){
                RSUAddressGotSwitched = true;
                System.out.println(carId + " can now only produce on RSU @ " + thirdRSUAddress );
            }
            nearestRSUAddress = thirdRSUAddress;
        }

        else{
            if(!nearestRSUAddress.equals(fourthRSUAddress)){
                RSUAddressGotSwitched = true;
                System.out.println(carId + " can now only produce on RSU @ " + fourthRSUAddress );
            }
            nearestRSUAddress = fourthRSUAddress;
        }
    }

    void updateCarProps(){
        position = position + (speed * tickTime);
        speed =  (int) ((Math.random() * (30)) + 15);
        automatedRSUAllocator();
    }

    private static final String firstRSUAddress = "localhost";                  //change these addresses to device addresses
    private static final String secondRSUAddress = "localhost";
    private static final String thirdRSUAddress = "localhost";
    private static final String fourthRSUAddress = "localhost";


}
