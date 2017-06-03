{-# LANGUAGE OverloadedStrings #-}
module AsyncExample.Main where

import Web.Scotty as Scotty
import Data.Aeson (object, (.=))
import Data.Aeson.Lens
import Control.Concurrent.STM.TBMQueue
import Control.Concurrent.STM
import Network.AWS as AWS
import Network.AWS.SES.SendEmail as AWS
import Control.Immortal
import System.Posix.Signals
import Network.Socket as Socket
import Control.Exception
import Data.Default
import Control.Monad
import Control.Lens (view, set, (^?))
import Control.Monad.IO.Class
import Data.Text
import Data.Foldable
import Network.AWS.SES.Types as AWS
import Network.HTTP.Types.Status

-- Create a socket.
bindSocketPort :: PortNumber -> IO Socket
bindSocketPort p =
  bracketOnError (socket AF_INET Stream defaultProtocol) close
    $ \sock -> do
      mapM_ (uncurry $ setSocketOption sock)
            [ (NoDelay  , 1)
            , (ReuseAddr, 1)
            ]
      bind sock $ SockAddrInet p $ tupleToHostAddress (127, 0, 0, 1)
      listen sock (max 2048 maxListenQueue)
      return sock

logExcept :: SomeException -> IO ()
logExcept = print

logFailedRequest :: MonadIO m => SendEmailResponse -> m ()
logFailedRequest resp = do
    let stat = view sersResponseStatus resp

    unless (stat >= 200 && stat < 300) $
      liftIO $ putStrLn $ "SES failed with status: " ++ show stat

makeEmail :: Text -> SendEmail
makeEmail email
  = sendEmail "async@example.com"
              (set dToAddresses [email] destination)
  $ message (content "Welcome!")
  $ set bText (Just $ content "You signed up!") AWS.body

-- Try to enqueue a new email. If the queue is at the max size,
-- log a warning
enqueueEmail :: TBMQueue SendEmail -> Text -> IO ()
enqueueEmail queue email = do
  msuccess <- atomically
            $ tryWriteTBMQueue queue
            $ makeEmail email

  for_ msuccess $ \success -> unless success $
    putStrLn "Failed to enqueue email! Increase bounded queue size!"

-- The worker sets up a exception handler and then starts a loop
-- to send emails
worker :: Thread -> Env -> TBMQueue SendEmail -> IO ()
worker thread env queue = do
  -- Make a loop enclosing the thread and queue vars.
  let go :: AWS ()
      go = do
        -- Block waiting for a new email to send
        mpayload <- liftIO $ atomically $ readTBMQueue queue
        case mpayload of
         -- Nothing means the queue is closed and empty.
         -- Stop the loop and kill the thread.
          Nothing -> liftIO $ stop thread
          Just payload -> do
            resp <- AWS.send payload
            logFailedRequest resp

            -- Start the loop again
            go

  handle logExcept $ runResourceT $ runAWS env go


-- Return a 400 if the email prop is missing from the json input.
missingEmailError :: ActionM a
missingEmailError = do
  status status400
  text "missing email"
  finish

main :: IO ()
main = do
  -- Config
  let maxQueueSize    = 100000
      numberOfWorkers = 2

  -- Try to find the credentials (e.g. by looking in ~/.aws)
  env    <- newEnv Discover
  -- Create the bounded queue
  queue  <- newTBMQueueIO maxQueueSize
  -- Create all the worker threads.
  -- We will use the thread references to wait for completion
  -- at the end of the program.
  threads <- replicateM numberOfWorkers
           $ create
           $ \thread -> worker thread env queue

  -- Create the socket and close it in the face of exceptions
  bracket
    (bindSocketPort 7000)
    close
    $ \sock -> do
       -- Close the socket on Ctrl-C
       -- Closing the listening socket begins graceful shutdown
       installHandler sigINT (CatchOnce $ close sock) Nothing

       -- Start the server
       scottySocket def sock $ do
         -- Async handler
         post "/user" $ do
           input <- Scotty.body
           email <- maybe missingEmailError return
                  $ input ^? key "email" . _String

           liftIO $ enqueueEmail queue email
           json $ object ["id" .= email]

         -- Sync handler
         post "/user-sync" $ do
           input <- Scotty.body
           email <- maybe missingEmailError return
                  $ input ^? key "email" . _String

           resp <- liftIO
                 $ runResourceT
                 $ runAWS env
                 $ AWS.send
                 $ makeEmail email

           logFailedRequest resp

           json $ object ["id" .= email]

  -- Close the queue. When the workers finish dequeuing all the
  -- emails the threads will shutdown.
  atomically $ closeTBMQueue queue
  -- Do not exit until all the threads of finish.
  -- This way we fail to send an enqueued emails.
  mapM_ wait threads
