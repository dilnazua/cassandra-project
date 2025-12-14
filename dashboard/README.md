# System Visualization Dashboard

## Running the Dashboard

1. Make sure you have all dependencies installed:
   ```bash
   pip install -r ../requirements.txt
   ```

2. Run the Streamlit app:
   ```bash
   streamlit run app.py
   ```

   Or from the project root:
   ```bash
   streamlit run dashboard/app.py
   ```

3. The dashboard will open in your browser (typically at `http://localhost:8501`)

## Usage

1. **Initialize the System**: Click "Initialize System" in the sidebar to create the distributed system

2. **Run Operations**:
   - Use the "Operations" section to execute PUT, GET, or DELETE operations
   - Use "Step Forward" to advance the simulation by one step
   - Use "Run 10 Steps" to advance by 10 steps at once

3. **Control Nodes**:
   - Select a node from the dropdown
   - Click "Fail Node" to simulate a node failure
   - Click "Recover Node" to bring a failed node back online

4. **Monitor the System**:
   - View the ring topology to see how keys are distributed
   - Check node data to see what each node stores
   - Monitor network statistics and gossip protocol state
