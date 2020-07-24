package interview;

import java.util.ArrayList;
import java.util.Random;
class CigaretteSmokersProblem {
 
    public static void main(String[] args) {
        table smokingtable = new table();
 
        agent controlAgent = new agent(smokingtable);
 
        controlAgent.start();
 
        for (int i = 0; i < 3; i++)
        {
            // each smoker-thread gets the controlAgent, so the thread can wake up the agent and we don't have to do notifyAll();
            smoker smokerThread = new smoker(smokingtable, i, "Smoker " + Integer.toString(i+1), controlAgent);
            smokerThread.start();
        }
    }
}
 
class smoker extends Thread {
 
    private table smokingtable = new table();
    private String ingredient;
    private int ingredientNumber;
    private agent controlAgent;
 
    public smoker(table pSmokingtable, int pIngredientNumber, String pName, agent pAgent)
    {
        // only save the number of the ingredient, we'll get the ingredient's name while the thread is running
        ingredientNumber = pIngredientNumber;
        this.smokingtable = pSmokingtable;
        setName(pName);
        controlAgent = pAgent;
    }
 
    //@Override
    public void run()
    {
        while(true)
        {
            ingredient = smokingtable.getSmokerIngredient(ingredientNumber);
 
            if (!smokingtable.hasIngredient(ingredient) && !smokingtable.isEmpty())
            {
                System.out.println(getName() + " has " + ingredient + ".");
                try {
                    doSmoke();
                    System.out.println(getName() + " tells the agent to start the next round.");
                    // the thread tells the agent to continue
                    controlAgent.wake();
                } catch (Exception e) {}
            }
        }
    }
 
    public synchronized void doSmoke() throws Exception
    {
        System.out.println(getName() + " rolls the cigarette.");
        Thread.sleep(2500);
        System.out.println(getName() + " smokes.");
        Thread.sleep(2500);
        System.out.println(getName() + " has finished.");
    }
 
  }
 
class agent extends Thread {
 
    private table smokingtable;
 
    public agent(table pSmokingtable)
    {
        smokingtable = pSmokingtable;
    }
 
    //@Override
    public void run()
    {
        while(true)
        {
            try {
                Thread.sleep(5000);
            } catch (Exception e) {}
            smokingtable.setAgentIngredients();
            // this triggers the smoker-threads to look at the table
            System.out.println("\n");
            System.out.println("The agents puts " + smokingtable.getAgentIngredients() + " on the table.");
            // pause the agent while one smoker thread is running
            pause();
        }
    }
 
    public synchronized void wake()
    {
        try
        {
            notify();
        } catch(Exception e){}
    }
 
 
    public synchronized void pause()
    {
        try
        {
            this.wait();
        } catch (Exception e) {}
    }
 
 
}
 
/*Class for creating table*/
class table {
    //Variables for storing ingredients(tabacco,paper,matches)
    private ArrayList allIngredients  = new ArrayList();
    private ArrayList agentIngredients = new ArrayList();
 
    public table()
    {
        allIngredients .add("tabacco");
        allIngredients .add("paper");
        allIngredients .add("matches");
    }
    /*Function for setting two ingredients randomly*/
    public void setAgentIngredients()
    {
        Random random = new Random();
 
        agentIngredients.clear();
 
        ArrayList copyAllElements = (ArrayList) allIngredients .clone();
 
        int ingredient1 = random.nextInt(copyAllElements.size());
        agentIngredients.add(copyAllElements.get(ingredient1));
 
        copyAllElements.remove(ingredient1);
        int ingredient2 = random.nextInt(copyAllElements.size());
        agentIngredients.add(copyAllElements.get(ingredient2));
    }
    /*Function for checking weather the table is empty*/
    public boolean isEmpty()
    {
        return (agentIngredients.size() == 0);
    }
    /*Function for getting two ingredients that 
    are set previously by the function setAgentIngredients()*/
    public synchronized String getAgentIngredients()
    {
        notifyAll();
        return agentIngredients.toString();
    }
    /*Function for getting the ingredient 
    with the corresponding smoker*/
    public synchronized String getSmokerIngredient(int pIngredient)
    {
        try {
            this.wait();
        } catch (Exception e) {}
        return (String) allIngredients .get(pIngredient);
    }
    /*Function for Checking the smoker has the same ingredient 
    as in the table if so smoking is is not possible*/
    public boolean hasIngredient(String ingredientName)
    {
        return (agentIngredients.contains(ingredientName));
    }
 
    public synchronized void pause()
    {
        try {
            this.wait();
        } catch (Exception e) {}
    }
}
