package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/CSC3104-ARCADE/pkg/uno"
)

// ANSI color escapes for vibrant terminal rendering
const (
	ansiReset  = "\033[0m"
	ansiBold   = "\033[1m"
	ansiRed    = "\033[1;31m"
	ansiYellow = "\033[1;33m"
	ansiGreen  = "\033[1;32m"
	ansiBlue   = "\033[1;34m"
	ansiWild   = "\033[1;35m"
)

func formatColor(c uno.Colour) string {
	switch c {
	case uno.ColourRed:
		return fmt.Sprintf("%sRED%s", ansiRed, ansiReset)
	case uno.ColourYellow:
		return fmt.Sprintf("%sYELLOW%s", ansiYellow, ansiReset)
	case uno.ColourGreen:
		return fmt.Sprintf("%sGREEN%s", ansiGreen, ansiReset)
	case uno.ColourBlue:
		return fmt.Sprintf("%sBLUE%s", ansiBlue, ansiReset)
	case uno.ColourWild:
		return fmt.Sprintf("%sWILD%s", ansiWild, ansiReset)
	default:
		return string(c)
	}
}

func formatCard(c uno.Card) string {
	var colorCode string
	switch c.Colour {
	case uno.ColourRed:
		colorCode = ansiRed
	case uno.ColourYellow:
		colorCode = ansiYellow
	case uno.ColourGreen:
		colorCode = ansiGreen
	case uno.ColourBlue:
		colorCode = ansiBlue
	default:
		colorCode = ansiWild
	}
	return fmt.Sprintf("%s[%s %s]%s", colorCode, strings.ToUpper(string(c.Colour)), c.Value, ansiReset)
}

func printHeader() {
	fmt.Printf("\n%s%s=======================================================%s\n", ansiBold, ansiYellow, ansiReset)
	fmt.Printf("%s         🃏 WELCOME TO CSC3104 ARCADE UNO 🃏          %s\n", ansiBold, ansiReset)
	fmt.Printf("%s%s=======================================================%s\n\n", ansiBold, ansiYellow, ansiReset)
	fmt.Println("Match started with 4 players: You, Bot-Alice, Bot-Bob, Bot-Charlie")
	fmt.Println("Type card number to play, 'd' to draw, 'p' to pass, 'snap' to test snapshot, or 'q' to quit.")
}

func printTableState(state *uno.State, cur string, allPlayers []string, humanPlayer string) {
	top, _ := state.TopCard()
	directionStr := "Clockwise (->)"
	if state.Direction < 0 {
		directionStr = "Counter-Clockwise (<-)"
	}

	fmt.Printf("-------------------------------------------------------\n")
	fmt.Printf("Top Card: %s  |  Color in force: %s  |  Dir: %s\n",
		formatCard(top), formatColor(state.CurrentColour), directionStr)

	var counts []string
	for _, p := range allPlayers {
		if p != humanPlayer {
			counts = append(counts, fmt.Sprintf("%s: %d cards", p, len(state.Hands[p])))
		}
	}
	fmt.Printf("Opponents: %s\n", strings.Join(counts, " | "))
	fmt.Printf("Active Player: %s%s%s\n", ansiBold, cur, ansiReset)
}

func promptDeclaredColor(reader *bufio.Reader) uno.Colour {
	fmt.Print("Declare Color (r = Red, y = Yellow, g = Green, b = Blue): ")
	cInput, _ := reader.ReadString('\n')
	cInput = strings.TrimSpace(strings.ToLower(cInput))
	switch cInput {
	case "r", "red":
		return uno.ColourRed
	case "y", "yellow":
		return uno.ColourYellow
	case "g", "green":
		return uno.ColourGreen
	case "b", "blue":
		return uno.ColourBlue
	default:
		fmt.Printf("%sInvalid color choice, defaulting to Red.%s\n", ansiYellow, ansiReset)
		return uno.ColourRed
	}
}

func handleHumanTurn(reader *bufio.Reader, engine *uno.Game, humanPlayer string) bool {
	state := engine.State()
	hand := state.Hands[humanPlayer]
	fmt.Printf("\nYour Hand (%d cards):\n", len(hand))
	for i, c := range hand {
		playable := ""
		if uno.IsPlayable(state, humanPlayer, c) {
			playable = fmt.Sprintf(" %s(Playable)%s", ansiGreen, ansiReset)
		}
		fmt.Printf("  [%d] %-20s%s\n", i, formatCard(c), playable)
	}

	fmt.Print("\nYour Move [index | 'd' to draw | 'p' to pass | 'q' to quit]: ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	switch input {
	case "q", "quit":
		fmt.Println("Thanks for playing!")
		return true
	case "snap":
		snap, _ := engine.ExportSnapshot()
		chk, _ := engine.Checksum()
		fmt.Printf("%s[SNAPSHOT]%s Canonical state exported: %d bytes | SHA256: %s\n", ansiYellow, ansiReset, len(snap), chk)
		return false
	case "d", "draw":
		if _, err := engine.DrawCard(humanPlayer); err != nil {
			fmt.Printf("%sError:%s %v\n", ansiRed, ansiReset, err)
		} else {
			fmt.Println("You drew a card.")
		}
		return false
	case "p", "pass":
		if _, err := engine.PassTurn(humanPlayer); err != nil {
			fmt.Printf("%sError:%s %v\n", ansiRed, ansiReset, err)
		} else {
			fmt.Println("You passed your turn.")
		}
		return false
	default:
		executeHumanPlay(reader, engine, humanPlayer, hand, input)
		return false
	}
}

func executeHumanPlay(reader *bufio.Reader, engine *uno.Game, humanPlayer string, hand []uno.Card, input string) {
	idx, err := strconv.Atoi(input)
	if err != nil || idx < 0 || idx >= len(hand) {
		fmt.Printf("%sInvalid choice.%s Please enter a valid card index, 'd', or 'p'.\n", ansiRed, ansiReset)
		return
	}

	chosen := hand[idx]
	var declaredCol uno.Colour
	if chosen.IsWild() {
		declaredCol = promptDeclaredColor(reader)
	}

	if _, err := engine.PlayCard(humanPlayer, idx, declaredCol); err != nil {
		fmt.Printf("%sIllegal move:%s %v\n", ansiRed, ansiReset, err)
	} else {
		fmt.Printf("You played %s!\n", formatCard(chosen))
	}
}

func handleBotTurn(engine *uno.Game, botName string) {
	time.Sleep(700 * time.Millisecond)
	state := engine.State()
	hand := state.Hands[botName]

	for idx, c := range hand {
		if uno.IsPlayable(state, botName, c) {
			var col uno.Colour
			if c.IsWild() {
				col = uno.ColourRed
			}
			if _, err := engine.PlayCard(botName, idx, col); err == nil {
				fmt.Printf("%s played %s\n", botName, formatCard(c))
				return
			}
		}
	}

	if _, err := engine.DrawCard(botName); err == nil {
		fmt.Printf("%s had no playable card and drew a card.\n", botName)
	} else {
		_, _ = engine.PassTurn(botName)
	}
}

func printGameOver(winner string, humanPlayer string) {
	fmt.Printf("\n%s%s=======================================================%s\n", ansiBold, ansiYellow, ansiReset)
	if winner == humanPlayer {
		fmt.Printf("🎉🎉 %sCONGRATULATIONS! YOU WON THE UNO MATCH!%s 🎉🎉\n", ansiGreen, ansiReset)
	} else {
		fmt.Printf("💀 GAME OVER! %s won the match!\n", winner)
	}
	fmt.Printf("%s%s=======================================================%s\n\n", ansiBold, ansiYellow, ansiReset)
}

func main() {
	connectURL := flag.String("connect", "", "WebSocket URL to join a remote gameserver or proxy (e.g. ws://localhost:8081/ws)")
	playerID := flag.String("player", "player1", "player identifier when connecting remotely")
	flag.Parse()

	printHeader()

	if *connectURL != "" {
		if err := runRemoteClient(context.Background(), *connectURL, *playerID); err != nil {
			fmt.Printf("%sRemote session ended: %v%s\n", ansiRed, err, ansiReset)
		}
		return
	}

	reader := bufio.NewReader(os.Stdin)

	humanPlayer := "You"
	botPlayers := []string{"Bot-Alice", "Bot-Bob", "Bot-Charlie"}
	allPlayers := []string{humanPlayer, botPlayers[0], botPlayers[1], botPlayers[2]}

	seed := time.Now().UnixNano()
	engine, err := uno.NewGameEngine("match-interactive", allPlayers, seed)
	if err != nil {
		fmt.Printf("Failed to deal game: %v\n", err)
		return
	}

	for !engine.IsFinished() {
		state := engine.State()
		cur := engine.CurrentPlayer()

		printTableState(state, cur, allPlayers, humanPlayer)

		if cur == humanPlayer {
			shouldQuit := handleHumanTurn(reader, engine, humanPlayer)
			if shouldQuit {
				return
			}
		} else {
			handleBotTurn(engine, cur)
		}
		fmt.Println()
	}

	printGameOver(engine.Winner(), humanPlayer)
}
