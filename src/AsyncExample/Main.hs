{-# LANGUAGE OverloadedStrings #-}
module AsyncExample.Main where

import Web.Scotty as Scotty
import Data.Aeson (object, (.=))
import Data.Aeson.Lens
import Control.Concurrent.STM.TBMQueue
import Control.Concurrent
import Control.Concurrent.STM
import Network.AWS
import Network.AWS.SES.SendEmail as AWS
import Control.Immortal
import System.Posix.Process
import System.Posix.Signals
import System.Posix.Types
import Network.Socket ( Socket, PortNumber, socket, close
                      , maxListenQueue, listen, tupleToHostAddress, SockAddr (..)
                      , bind, SocketOption (..), setSocketOption, defaultProtocol
                      , SocketType (..), Family (..)
                      )
import Control.Exception
import Data.Default
import Control.Monad
import Control.Lens (view, set, (^?))
import Control.Monad.IO.Class
import Data.Text
import Data.Foldable
import Network.AWS.SES.Types as AWS
import Network.HTTP.Types.Status

logAnyError :: SomeException -> IO ()
logAnyError = print

bindSocketPort :: PortNumber -> IO Socket
bindSocketPort p =
  bracketOnError (socket AF_INET Stream defaultProtocol) close $ \sock -> do
    mapM_ (uncurry $ setSocketOption sock)
          [ (NoDelay  , 1)
          , (ReuseAddr, 1)
          ]
    bind sock $ SockAddrInet p $ tupleToHostAddress (127, 0, 0, 1)
    listen sock (max 2048 maxListenQueue)
    return sock

worker :: Thread -> Env -> TBMQueue SendEmail -> IO ()
worker thread env queue = handle logAnyError $ runResourceT $ runAWS env $ go where
     go :: AWS ()
     go = do
       mpayload <- liftIO $ atomically $ readTBMQueue queue
       case mpayload of
         Nothing -> liftIO $ stop thread
         Just payload -> do
           result <- send payload
           let status = view sersResponseStatus result

           unless (status >= 200 && status < 300) $
             liftIO $ putStrLn $ "SES failed with status: " ++ show status

           go

makeEmail :: Text -> SendEmail
makeEmail email
  = sendEmail "async@example.com" (set dToAddresses [email] destination)
  $ message (content "Welcome!")
  $ set bText (Just $ content "You signed up!") AWS.body

enqueueEmail :: TBMQueue SendEmail -> Text -> IO ()
enqueueEmail queue email = do
  msuccess <- atomically
            $ tryWriteTBMQueue queue
            $ makeEmail email

  for_ msuccess $ \success ->
    unless success $ print "Failed to enqueue email! Increase bounded queue size!"

missingEmailError = status status400 >> text "missing email" >> finish

main :: IO ()
main = do
  let maxQueueSize    = 100000
      numberOfWorkers = 1

  env    <- newEnv Discover
  queue  <- newTBMQueueIO maxQueueSize
  threads <- replicateM numberOfWorkers
           $ create
           $ \thread -> worker thread env queue

  bracket
    (bindSocketPort 7000)
    close
    $ \sock -> do
       installHandler sigINT (CatchOnce $ close sock) Nothing

       scottySocket def sock $ do
         post "/user" $ do
           input <- Scotty.body
           email <- maybe missingEmailError return
                  $ input ^? key "email" . _String

           liftIO $ enqueueEmail queue email
           json $ object ["id" .= email]

         post "/user-sync" $ do
           input <- Scotty.body
           email <- maybe missingEmailError return
                  $ input ^? key "email" . _String

           response <- liftIO $ runResourceT $ runAWS env $ send $ makeEmail email
           let status = view sersResponseStatus response

           unless (status >= 200 && status < 300) $
             liftIO $ putStrLn $ "SES failed with status: " ++ show status

           json $ object ["id" .= email]

  atomically $ closeTBMQueue queue
  mapM_ wait threads

