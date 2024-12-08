#!/bin/bash

# Function to detect the available terminal emulator
detect_terminal() {
    if command -v gnome-terminal &> /dev/null; then
        echo "gnome-terminal"
    elif command -v konsole &> /dev/null; then
        echo "konsole"
    elif command -v xterm &> /dev/null; then
        echo "xterm"
    elif command -v terminator &> /dev/null; then
        echo "terminator"
    else
        echo "none"
    fi
}

# Detect the terminal emulator
TERMINAL=$(detect_terminal)

if [ "$TERMINAL" = "none" ]; then
    echo "No supported terminal emulator found. Please install gnome-terminal, konsole, xterm, or terminator."
    exit 1
fi

# Function to start a server with given ID
start_server() {
    local id=$1
    local title="DUR Server $id"
    
    case $TERMINAL in
        ("gnome-terminal")
            gnome-terminal --title="$title" -- bash -c "python3 Server.py --server-id $id; exec bash"
            ;;
        ("konsole")
            konsole --new-tab --title "$title" -e bash -c "python3 Server.py --server-id $id; exec bash"
            ;;
        ("xterm")
            xterm -T "$title" -e bash -c "python3 Server.py --server-id $id; exec bash" &
            ;;
        ("terminator")
            terminator -T "$title" -e "python3 Server.py --server-id $id" &
            ;;
    esac
    
    #sleep 1  # Wait a bit between server starts to avoid port conflicts
}

echo "Starting servers using $TERMINAL..."

# Start servers with IDs 0 through 5
for id in {0..5}
do
    echo "Starting server $id..."
    start_server $id
done

echo "All servers started."