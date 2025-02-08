open System
open Akka.FSharp

open Gossip

[<EntryPoint>]
let main argv =
    let system = System.create "my-system" (Configuration.load())

    // Number of times any single node should heard the rumor before stop transmitting it
    let maxCount = 10
    
    // Parse command line arguments
    let topology = argv.[1]
    let numNodes = roundNodes (int argv.[0]) topology
    let algorithm = argv.[2]
    let filepath = "results/" + topology + "-" + string numNodes + "-" + algorithm + ".csv"
    
    // Create topology
    let topologyMap = buildTopology numNodes topology

    // Initialize stopwatch
    let stopWatch = Diagnostics.Stopwatch()

    // Spawn the counter actor
    let counterRef = spawn system "counter" (counter 0 numNodes filepath stopWatch)

    // Run an algorithm based on user input
    match algorithm with
    | "gossip" ->
        // Gossip Algorithm
        // Create desired number of workers and randomly pick 1 to start the algorithm
        let workerRef =
            [ 1 .. numNodes ]
            |> List.map (fun nodeID ->
                let name = "worker" + string nodeID
                spawn system name (gossip maxCount topologyMap nodeID counterRef))
            |> pickRandom
        // Start the timer
        stopWatch.Start()
        // Send message
        workerRef <! "heardRumor"

    | "pushsum" ->
        // Push Sum Algorithm
        let workerRef =
            [ 1 .. numNodes ]
            |> List.map (fun nodeID ->
                let name = "worker" + string nodeID
                (spawn system name (pushSum topologyMap nodeID counterRef)))
        // Start the timer
        stopWatch.Start()
        // Send message
        workerRef |> List.iter (fun item -> item <! Initialize)


    // Wait till all the actors are terminated
    system.WhenTerminated.Wait()
    0 // return an integer exit code
